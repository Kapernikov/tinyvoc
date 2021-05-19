from tinyvoc.pvocutils import PascalVocObject
from tinyvoc import PascalVocAnnotation
from typing import List, Tuple

def overlapping_objects(nodes: List[PascalVocObject]) -> List[Tuple[PascalVocObject, PascalVocObject]]:
    aj = []
    for i in range(len(nodes)):
        test_node = nodes[i]
        the_rest = nodes[i+1:]
        for n in the_rest:
            if test_node.boundingbox.overlaps(n.boundingbox):
                aj.append( (test_node, n))
    return aj


def dfs(start, adjacency_list, nodes):
    """ref: http://code.activestate.com/recipes/576723/"""
    path = []
    q = [start]

    while q:
        node = q.pop(0)

        # cycle detection
        if path.count(node) >= nodes.count(node):
            continue

        path = path + [node]

        # get next nodes
        next_nodes = [p2 for p1,p2 in adjacency_list if p1 == node]
        q = next_nodes + q

    return path


def connected_components(annot: PascalVocAnnotation) -> List[List[PascalVocObject]]:
    L = annot.objects[:]
    AJ = overlapping_objects(L)
    Cs = []
    while len(L) > 0:
        N = L[0]
        C = dfs(N, AJ, L)
        Cs.append(C)
        for n in C:
            L.remove(n)
    return Cs

def overlap_factor(annot: PascalVocAnnotation):
    return float(len(annot.objects)) / float(len(connected_components(annot)))