from prefect import task

import functions

create_folder_structure = task(functions.create_folder_structure)
load_dublin_boundary = task(functions.load_file, name="Load Dublin Boundary")
load_dublin_small_area_boundaries = task(
    functions.load_file, name="Load Dublin Small Area Boundaries"
)
dissolve_boundaries_to_polygon = task(
    functions.dissolve_geometries_to_shapely_polygon,
    name="Dissolve Boundaries to Shapely Polygon",
)
load_roads = task(functions.load_roads, name="Load Roads")

extract_lines = task(functions.extract_lines, name="Extract Lines")
cut_lines_on_boundaries = task(
    functions.cut_lines_on_boundaries, name="Cut Lines on Boundaries"
)

save_to_gpkg = task(functions.save_to_gpkg, name="Save to File")
