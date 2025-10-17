import py_compile, sys
try:
    py_compile.compile(r'Data_analytics/scripts/poc_assign_single_game.py', doraise=True)
    print('OK')
except Exception as e:
    print('ERROR:', e)
    sys.exit(1)
