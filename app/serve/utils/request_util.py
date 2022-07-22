def get_data_args(args: dict):
    separator = args.get('__separator', ',')
    as_type = args.get('__as', 'csv')
    where = args.get('__where', '1=1')
    axis = int(args.get('__axis', 1))

    return separator, as_type, where, axis, {key.replace("_", ""): value for key, value in args.items() if key.startswith("__") and key.endswith("__")}
