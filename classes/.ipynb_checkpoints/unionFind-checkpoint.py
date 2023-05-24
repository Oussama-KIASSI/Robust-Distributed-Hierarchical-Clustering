from classes.edge import Edge
from classes.point import Point
class UnionFind:
    def __init__(self, N):
        if N < 0:
            raise ValueError("N must be non-negative")
        self.id = [i for i in range(N)]
        self.count = N

    #find takes a vertice p and keeps tracking the list of vertices and their parents "self.id ==> self.id[p] = p_parent" until it gets the root of the vertice p.
    #find return the root of the vertice given as a parameter
    def find(self, p):
        if p < 0 or p >= len(self.id):
            print("p = ", p)
            raise ValueError("p is out of range")
        while p != self.id[p]:
            p = self.id[p]
        return p

    def get_components(self):
        return self.count

    def is_connected(self, left, right):
        i = self.find(left)
        j = self.find(right)
        return i == j

    # unify takes two elements, optimize the path to their root by using compress_path function
    #the oprimization is done in a way that each element of the component with the lower number of elements is chenged to be the root of the larger component
    #finally, the functuion returns false if the two elements belong to the same component which means that no mergins was done.otherwise it returns True and decrimente self.count
    def unify(self, left, right):
        i = self.find(left)
        j = self.find(right)
        big = i if i > j else j
        self.compress_path(left, big)
        self.compress_path(right, big)
        if i == j:
            return False
        else:
            self.count -= 1
            return True
    # compress_path purpose is to optimize the find method by compressing the paths of all the elements in the same component as the input element "element".
    #it changes the parent element of each of "element" ancestor so it points directly to the root element "big" which is the root of the component with larger components in the union function test
    def compress_path(self, element, big):
        p = element
        while self.id[p] != p:
            parent = self.id[p]
            self.id[p] = big
            p = parent
        self.id[p] = big
