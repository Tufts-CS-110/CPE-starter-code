import os
import json
import argparse
from graph import Graph


def find_root_span_info(trace_data):
    """Finds the root span (spanID == traceID) and returns its
    serviceName and operationName.
    """
    for trace in trace_data['data']:
        trace_id = trace['traceID']
        spans = trace['spans']
        processes = trace['processes']

        for span in spans:
            if span['spanID'] == trace_id:
                process_id = span['processID']
                service_name = processes[process_id]['serviceName']
                operation_name = span['operationName']
                return service_name, operation_name

    print("NO ROOT SPAN FOUND!")
    return None, None


def process(trace_dir, out_dir, format):
    for file in os.listdir(trace_dir):
        if file.endswith(".json"):
            file_path = os.path.join(trace_dir, file)
            file_name = os.path.splitext(file)[0]
            print(f"Processing trace file: {file_path}")

            with open(file_path, 'r') as f:
                trace_data = json.load(f)

            serviceName, operationName = find_root_span_info(trace_data)
            graph = Graph(trace_data, serviceName,
                          operationName, file, True)
            critical_path = graph.findCriticalPath()

            out_txt = os.path.join(out_dir, file_name + ".txt")
            out_dot = os.path.join(out_dir, file_name + ".dot")

            if format == "txt":
                graph.output_cpe_txt(out_txt)
            elif format == "dot":
                graph.output_cpe_dot(out_dot)
            elif format == "both":
                graph.output_cpe_txt(out_txt)
                graph.output_cpe_dot(out_dot)


def parse_arguments():
    parser = argparse.ArgumentParser(
        description="Convert trace JSON files to critical path representations in TXT or DOT format."
    )
    parser.add_argument(
        "trace_dir", type=str, help="Directory containing trace JSON files."
    )
    parser.add_argument(
        "out_dir", type=str, help="Directory to save the output files."
    )
    parser.add_argument(
        "format",
        type=str,
        choices=["txt", "dot", "both"],
        help="Output format: 'txt', 'dot', or 'both'.",
    )
    return parser.parse_args()


def main():
    args = parse_arguments()
    process(args.trace_dir, args.out_dir, args.format)


if __name__ == "__main__":
    main()
