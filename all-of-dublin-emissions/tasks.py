import prefect

import functions


check_file_exists = prefect.task(functions.check_file_exists)
create_folder_structure = prefect.task(functions.create_folder_structure)
