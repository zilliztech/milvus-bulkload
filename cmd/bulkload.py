import sys
import time
from pymilvus import (
    connections,
    utility,
    BulkLoadState,
)

# Milvus service address
_HOST = '127.0.0.1'
_PORT = '19530'

# Create a Milvus connection
def create_connection():
    try:
        connections.connect(host=_HOST, port=_PORT)
    except Exception as e:
        print("Cannot connect to Milvus. Error: " + str(e))


# Get bulkload task state to check whether the data file has been parsed and persisted successfully.
# Persisted state doesn't mean the data is queryable, to query the data, you need to wait until the segment is
# loaded into memory.
def wait_tasks_persisted(task_ids):
    print("=========================================================================================================")
    states = wait_tasks_to_state(task_ids, BulkLoadState.ImportPersisted)
    persist_count = 0
    for state in states:
        if state.state == BulkLoadState.ImportPersisted:
            persist_count = persist_count + 1
        # print(state)
        # if you want to get the auto-generated primary keys, use state.ids to fetch
        # print("Auto-generated ids:", state.ids)

    print(persist_count, "of", len(task_ids), " tasks have finished parsing and persisting")
    print("=========================================================================================================\n")
    return states

# wait all bulkload tasks to be a certain state
# return the states of all the tasks, including failed task
def wait_tasks_to_state(task_ids, state_code):
    wait_ids = task_ids
    states = []
    while True:
        time.sleep(2)
        temp_ids = []
        for id in wait_ids:
            state = utility.get_bulk_load_state(task_id=id)
            if state.state == BulkLoadState.ImportFailed:
                print(state)
                print("The task", state.task_id, "failed, reason:", state.failed_reason)
                continue

            if state.state == state_code:
                states.append(state)
                continue

            temp_ids.append(id)

        wait_ids = temp_ids
        if len(wait_ids) == 0:
            break;
        print(len(wait_ids), "tasks not reach state:", BulkLoadState.state_2_name.get(state_code, "unknown"), ", next round check")

    return states

def bulk_load(collection_name: str, partition_name: str, is_row_based: bool, files: list):
    task_ids = utility.bulk_load(collection_name=collection_name,partition_name=partition_name,is_row_based=is_row_based, files=files)
    return wait_tasks_persisted(task_ids)

def main():
    create_connection()
    collection_name = sys.argv[1]
    partition_name = sys.argv[2]
    is_row_based = True if sys.argv[3] == "true" else False
    files = sys.argv[4:]
    bulk_load(collection_name,partition_name,is_row_based,files)
   

if __name__ == '__main__':
    main()