from prefect import task

import functions

create_folder_structure = task(functions.create_folder_structure)
load_dublin_boundary = task(functions.load_file, name="Load Dublin Boundary")
dissolve_boundaries_to_polygon = task(
    functions.dissolve_geometries_to_shapely_polygon,
    name="Dissolve Boundaries to Shapely Polygon",
)
load_roads = task(functions.load_roads, name="Load Roads")
