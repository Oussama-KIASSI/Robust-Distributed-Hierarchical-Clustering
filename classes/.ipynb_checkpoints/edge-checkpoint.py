class Edge:
    def __init__(self, end1, end2, weight):
        self.left = end1
        self.right = end2
        self.weight = weight

    def get_left(self):
        return self.left

    def get_right(self):
        return self.right

    def get_weight(self):
        return self.weight

    def __str__(self):
        return "[(%d %d) %f]" % (self.left, self.right, self.weight)
