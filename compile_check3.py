import py_compile, sys
try:
    py_compile.compile(r'Data_analytics/src/utils/mlflow_utils.py', doraise=True)
    py_compile.compile(r'Data_analytics/src/pipelines/event_detection/detect_events.py', doraise=True)
    py_compile.compile(r'Data_analytics/src/pipelines/event_detection/enrich_events.py', doraise=True)
    py_compile.compile(r'Data_analytics/src/pipelines/ccf_analysis/analyze_competitors_ccf.py', doraise=True)
    py_compile.compile(r'Data_analytics/scripts/summarize_topics_column.py', doraise=True)
    print('OK')
except Exception as e:
    print('ERROR:', e)
    sys.exit(1)
