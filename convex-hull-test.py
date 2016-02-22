#!flask/bin/python3

import random

def _is_right_turn(p, q, r):
    """
    Do the vectors pq:qr form a right turn?
    """
    # Use cross product
    v1x = q['x_cord'] - p['x_cord']
    v1y = q['y_cord'] - p['y_cord']
    v2x = r['x_cord'] - q['x_cord']
    v2y = r['y_cord'] - q['y_cord']

    if v1x * v2y - v1y * v2x > 0:
        return False
    else:
        return True

def compute_convex_hull_gift_wrapping(labeled_points):
    print("Begin gift wrapping!")
    # find left-most point
    leftmost_index = 0
    for i in range(len(labeled_points)):
        if labeled_points[i]['x_cord'] < labeled_points[leftmost_index]['x_cord']:
            leftmost_index = i
    point_on_hull = labeled_points[leftmost_index]
    convex_hull = []
    i = 0
    while True:
        convex_hull.append(point_on_hull)
        end_point = labeled_points[0]
        for j in range(len(labeled_points)):
            if end_point == point_on_hull or not _is_right_turn(convex_hull[i], end_point, labeled_points[j]):
                end_point = labeled_points[j]
        i += 1
        point_on_hull = end_point

        print("labeled points length: " + str(len(labeled_points)))
        print("Convex Hull Length: " + str(len(convex_hull)))
        if end_point == convex_hull[0]:
            break
    print("Finish gift wrapping!")
    return convex_hull

if __name__ == '__main__':
    points = [{'x_cord': random.random(), 'y_cord': random.random()} for i in range(100)]
    #points = [(random.random(), random.random()) for i in range(1000)]
    #points = [(random.random(), random.random()) for i in range(10000)]
    print(compute_convex_hull_gift_wrapping(points))