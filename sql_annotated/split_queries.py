import os
import sys

def split_queries(input_file, output_directory):
    os.makedirs(output_directory, exist_ok=True)

    with open(input_file, "r") as file:
        lines = [line for line in file if not line.strip().startswith("--")]
        sql_text = "\n".join(lines)
        queries = [q.strip() + ";" for q in sql_text.split(";") if q.strip()]

    for i, query in enumerate(queries, start=1):
        query = query.strip()
        if query:
            output_file = os.path.join(output_directory, f"query_{i}.sql")
            with open(output_file, "w") as f:
                f.write(query)
    
    print(f"Queries have been split and saved in '{output_directory}'.")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python split_queries.py <input_file> <output_directory>")
        sys.exit(1)

    split_queries(sys.argv[1], sys.argv[2])
    
