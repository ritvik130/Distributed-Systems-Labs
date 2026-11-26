import json

with open("coordinator-state.json") as file:
    state = json.load(file)

for phase in ("Map", "Reduce"):
    print(f"\n{phase.upper()} TASKS")
    print(f"{'Task':<6} {'State':<13} {'Issued':<8} {'Winner':<8} Attempts (ID:Worker)")
    for i, task in enumerate(state[phase + "Tasks"]):
        attempts = task["Attempts"] or {}
        owners = ", ".join(f"{a}:W{v['WorkerID']}" for a, v in attempts.items())
        winner = task["AcceptedAttempt"] or "-"
        print(f"{i:<6} {task['State']:<13} {task['AttemptsIssued']:<8} {winner!s:<8} {owners or '-'}")

    print("Total attempts:", sum(t["AttemptsIssued"] for t in state[phase + "Tasks"]))
