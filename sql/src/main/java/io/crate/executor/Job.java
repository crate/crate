package io.crate.executor;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class Job {

    private final UUID id = UUID.randomUUID();
    private List<Task> tasks = new ArrayList<>();

    public UUID id() {
        return id;
    }

    public void addTask(Task task) {
        tasks.add(task);
    }

    public List<Task> tasks() {
        return tasks;
    }
}
