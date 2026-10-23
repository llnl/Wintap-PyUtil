import argparse
import os

import pyarrow as pa
import pyarrow.parquet as pq


def add_required_to_all(source_file, dest_dir):
    src = pq.read_table(source_file)

    schema = src.schema
    for i, col in enumerate(schema):
        schema = schema.set(i, pa.field(col.name, col.type, nullable=False))

    writer = pq.ParquetWriter(
        os.path.join(dest_dir, os.path.basename(source_file) + "-required.parquet"),
        schema=schema,
    )
    src = src.cast(schema)
    writer.write_table(src)
    writer.close()


def main():
    parser = argparse.ArgumentParser(
        prog="add_required.py",
        description="Add required flag to every column of each parquet file",
    )
    parser.add_argument(
        "-s",
        "--source",
        help="Path of parquet files or a single file",
    )
    parser.add_argument(
        "-d",
        "--destination",
        help="Path for new parquet files",
    )
    args = parser.parse_args()

    if os.path.isdir(args.source):
        # Find all parquet files in the given path
        for path, _, files in os.walk(args.source):
            for name in files:
                if name.endswith(".parquet"):
                    print(f"Processing: {path}   {name}")
                    add_required_to_all(os.path.join(path, name), args.destination)
    else:
        add_required_to_all(args.source, args.destination)


if __name__ == "__main__":
    main()
