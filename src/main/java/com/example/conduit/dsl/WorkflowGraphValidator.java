package com.example.conduit.dsl;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Validates a parsed {@link WorkflowGraph} at definition-create time. Collects every error, then
 * throws once — so the API can report all problems in one response. Pure; no infra.
 */
public class WorkflowGraphValidator {

    public void validate(WorkflowGraph graph) {
        List<String> errors = new ArrayList<>();
        Map<String, State> states = graph.states() == null ? Map.of() : graph.states();

        if (graph.startAt() == null || !states.containsKey(graph.startAt())) {
            errors.add("StartAt '" + graph.startAt() + "' is not a declared state");
        }

        states.forEach((name, state) -> {
            for (String target : transitions(state)) {
                if (!states.containsKey(target)) {
                    errors.add("State '" + name + "' transitions to unknown state '" + target + "'");
                }
            }
            checkRequiredFields(name, state, errors);
        });

        // Graph-traversal rules need every transition to resolve first, else they would NPE on a
        // dangling target. Only run them once the structure above is sound.
        if (errors.isEmpty()) {
            Set<String> reachable = reachableFrom(graph.startAt(), states);
            if (reachable.stream().noneMatch(n -> isTerminal(states.get(n)))) {
                errors.add("no terminal state (Succeed/Fail/End) is reachable from StartAt");
            }
            findInstantCycle(reachable, states)
                    .ifPresent(cycle -> errors.add("instant-state cycle (would not reach a parking "
                            + "state, spinning decide()): " + String.join(" -> ", cycle)));
        }

        if (!errors.isEmpty()) {
            throw new DslValidationException(errors);
        }
    }

    /** States reachable from {@code start} by following declared transitions. */
    private Set<String> reachableFrom(String start, Map<String, State> states) {
        Set<String> seen = new LinkedHashSet<>();
        Deque<String> queue = new ArrayDeque<>();
        queue.add(start);
        seen.add(start);
        while (!queue.isEmpty()) {
            for (String next : transitions(states.get(queue.poll()))) {
                if (seen.add(next)) {
                    queue.add(next);
                }
            }
        }
        return seen;
    }

    /**
     * Finds a cycle composed entirely of instant states (Pass/Choice) among the reachable states.
     * Such a cycle would spin {@code decide()} forever since no iteration parks. A cycle that routes
     * through any parking state (Task/Wait/Parallel/Map) is legal and ignored. DFS with a recursion
     * stack; returns the offending path if one exists.
     */
    private java.util.Optional<List<String>> findInstantCycle(Set<String> reachable, Map<String, State> states) {
        Set<String> done = new HashSet<>();
        for (String node : reachable) {
            if (isInstant(states.get(node)) && !done.contains(node)) {
                List<String> path = new ArrayList<>();
                if (dfsInstantCycle(node, states, new LinkedHashSet<>(), done, path)) {
                    return java.util.Optional.of(path);
                }
            }
        }
        return java.util.Optional.empty();
    }

    private boolean dfsInstantCycle(String node, Map<String, State> states,
                                    Set<String> onStack, Set<String> done, List<String> path) {
        onStack.add(node);
        path.add(node);
        for (String target : transitions(states.get(node))) {
            if (!isInstant(states.get(target))) {
                continue; // edge leaves the instant subgraph — parking breaks any spin
            }
            if (onStack.contains(target)) {
                path.add(target);
                return true;
            }
            if (!done.contains(target) && dfsInstantCycle(target, states, onStack, done, path)) {
                return true;
            }
        }
        onStack.remove(node);
        path.remove(path.size() - 1);
        done.add(node);
        return false;
    }

    private boolean isInstant(State state) {
        return state instanceof PassState || state instanceof ChoiceState;
    }

    private boolean isTerminal(State state) {
        return switch (state) {
            case SucceedState ignored -> true;
            case FailState ignored -> true;
            case TaskState s -> s.end();
            case PassState s -> s.end();
            case WaitState s -> s.end();
            case ParallelState s -> s.end();
            case MapState s -> s.end();
            case ChoiceState ignored -> false;
        };
    }

    /** Per-state required-field rules. A non-terminal transition state must declare Next or End. */
    private void checkRequiredFields(String name, State state, List<String> errors) {
        switch (state) {
            case TaskState s -> {
                if (isBlank(s.resource())) {
                    errors.add("Task '" + name + "' requires a Resource");
                }
                requireNextOrEnd(name, s.next(), s.end(), errors);
            }
            case PassState s -> requireNextOrEnd(name, s.next(), s.end(), errors);
            case WaitState s -> {
                if ((s.seconds() == null) == (s.secondsPath() == null)) {
                    errors.add("Wait '" + name + "' requires exactly one of Seconds or SecondsPath");
                }
                requireNextOrEnd(name, s.next(), s.end(), errors);
            }
            case ParallelState s -> requireNextOrEnd(name, s.next(), s.end(), errors);
            case MapState s -> requireNextOrEnd(name, s.next(), s.end(), errors);
            case ChoiceState s -> {
                if (s.choices() == null || s.choices().isEmpty()) {
                    errors.add("Choice '" + name + "' requires at least one rule in Choices");
                }
            }
            case FailState s -> {
                if (s.error() == null) {
                    errors.add("Fail '" + name + "' requires an Error");
                }
            }
            case SucceedState ignored -> {
                // no required fields
            }
        }
    }

    private void requireNextOrEnd(String name, String next, boolean end, List<String> errors) {
        if (next == null && !end) {
            errors.add("State '" + name + "' must have Next or End:true");
        }
    }

    private boolean isBlank(String s) {
        return s == null || s.isBlank();
    }

    /** All state names this state can transition to (Next, Catch targets, Choice targets/Default). */
    private List<String> transitions(State state) {
        List<String> targets = new ArrayList<>();
        switch (state) {
            case TaskState s -> {
                targets.add(s.next());
                addCatchTargets(targets, s.catchers());
            }
            case PassState s -> targets.add(s.next());
            case WaitState s -> targets.add(s.next());
            case ChoiceState s -> {
                if (s.choices() != null) {
                    s.choices().forEach(rule -> targets.add(rule.next()));
                }
                targets.add(s.defaultNext());
            }
            case ParallelState s -> {
                targets.add(s.next());
                addCatchTargets(targets, s.catchers());
            }
            case MapState s -> {
                targets.add(s.next());
                addCatchTargets(targets, s.catchers());
            }
            case SucceedState ignored -> {
                // terminal: no transitions
            }
            case FailState ignored -> {
                // terminal: no transitions
            }
        }
        targets.removeIf(Objects::isNull);
        return targets;
    }

    private void addCatchTargets(List<String> targets, List<Catcher> catchers) {
        if (catchers != null) {
            catchers.forEach(c -> targets.add(c.next()));
        }
    }
}
