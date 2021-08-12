import prefect

import functions

check_file_exists = prefect.task(functions.check_file_exists)
