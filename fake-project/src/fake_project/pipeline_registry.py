from kedro.pipeline import Pipeline, node


def identity(arg):
    return arg


def register_pipelines():
    pipeline = Pipeline(
        [
            node(identity, ["input"], ["intermediate"], name="node0", tags=["tag0", "tag1"]),
            node(identity, ["intermediate"], ["output"], name="node1"),
            node(identity, ["intermediate"], ["output2"], name="node2", tags=["tag0"]),
            node(identity, ["intermediate"], ["output3"], name="node3", tags=["tag1", "tag2"]),
            node(identity, ["intermediate"], ["output4"], name="node4", tags=["tag2"]),
        ],
        tags="pipeline0",
    )
    return {
        "__default__": pipeline,
        "ds": pipeline,
    }
