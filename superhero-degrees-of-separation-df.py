from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("DegreesOfSeparation")
sc = SparkContext(conf=conf)

START_CHARACTER_ID = 5306
TARGET_CHARACTER_ID = 14
MAX_ITERATIONS = 10

hit_counter = sc.accumulator(0)

def convert_to_bfs(line):
    fields = line.split()
    hero_id = int(fields[0])
    connections = [int(connection) for connection in fields[1:]]
    color = "WHITE"
    distance = 9999

    if hero_id == START_CHARACTER_ID:
        color = "GRAY"
        distance = 0
    return (hero_id, (connections, distance, color))

def create_starting_rdd():
    input = sc.textFile("MarvelGraph.txt")
    return input.map(convert_to_bfs)

def map_bfs(node):
    character_id = node[0]
    data = node[1]
    connections = data[0]
    distance = data[1]
    color = data[2]

    results = []

    if color == "GRAY":
        for connection in connections:
            new_character_id = connection
            new_distance = distance + 1
            new_color = "GRAY"
            if TARGET_CHARACTER_ID == connection:
                hit_counter.add(1)

            new_entry = (new_character_id, ([], new_distance, new_color))
            results.append(new_entry)

        color = "BLACK"

    results.append((character_id, (connections, distance, color)))
    return results

def bfs_reduce(data1, data2):
    """
    Reduce function for breadth-first search algorithm.

    Args:
      data1 (tuple): Tuple containing the edges, distance, and color of the first data.
      data2 (tuple): Tuple containing the edges, distance, and color of the second data.

    Returns:
      tuple: Tuple containing the combined edges, minimum distance, and color.

    """
    edges1 = data1[0]
    edges2 = data2[0]
    distance1 = data1[1]
    distance2 = data2[1]
    color1 = data1[2]
    color2 = data2[2]

    distance = 9999
    color = color1
    edges = []

    if len(edges1) > 0:
        edges.extend(edges1)
    
    if len(edges2) > 0:
        edges.extend(edges2)
    
    if distance1 < distance:
        distance = distance1
    if distance2 < distance:
        distance = distance2
    
    # preserve the darkest color
    color_map = {"WHITE": 0, "GRAY": 1, "BLACK": 2}
    if color_map[color1] < color_map[color2]:
        color = color2

    if color_map[color1] > color_map[color2]:
        color = color1
    return (edges, distance, color)

### MAIN FUNCTION ###
iteration_rdd = create_starting_rdd()
# print(iteration_rdd.take(10))
for iteration in range(0, MAX_ITERATIONS):
    print(f"Running BFS iteration #{iteration + 1}")
    mapped = iteration_rdd.flatMap(map_bfs)

    # mapped.count() action here forces the RDD to be evaluated
    # so that the accumulator is actually updated.
    print(f"Processing {mapped.count()} values.")

    if hit_counter.value > 0:
        print(f"Hit the target character! From {hit_counter.value} different directions.")
        break
    
    ## Reducer combines data for each character ID, preserving the shortest distance and darkest color.
    iteration_rdd = mapped.reduceByKey(bfs_reduce)


   
