import operator
from scipy.spatial.distance import cdist
from typing import Callable, Tuple
from scipy.spatial.distance import euclidean
import numpy as np

class STFrechet:
    def __init__(self, a: np.ndarray, b: np.ndarray, dist: Callable[[np.ndarray, np.ndarray], float]):
        self.a = a
        self.b = b
        self.dist_func = dist
        self.dist_matrix = cdist(a, b, self.dist_func)
        self.cost, self.path, self.C, self.P = self.full()

    def move(self, a_remove: int = 0, b_remove: int = 0, a_add: np.ndarray = None, b_add: np.ndarray = None):
        next_start: Tuple[int, int] = (a_remove, b_remove)
        if next_start not in self.path:
            if a_remove > 0:
                self.a = self.a[a_remove:]
            if b_remove > 0:
                self.b = self.b[b_remove:]
            if a_add is not None:
                self.a = np.vstack([self.a, a_add])
            if b_add is not None:
                self.b = np.vstack([self.b, b_add])

            self.dist_matrix = cdist(self.a, self.b, self.dist_func)

            self.cost, self.path, self.C, self.P = self.full()

            return self.cost
        else:
            if a_add is not None or b_add is not None:
                self.cost = self.add(a_add, b_add)
            next_start: Tuple[int, int] = (a_remove, b_remove)
            if next_start in self.path:
                if a_remove > 0 or b_remove > 0:
                    self.cost = self.remove(a_remove, b_remove)
            else:
                if a_remove > 0 or b_remove > 0:
                    if a_remove > 0:
                        self.a = self.a[a_remove:]
                    if b_remove > 0:
                        self.b = self.b[b_remove:]
                    self.dist_matrix = cdist(self.a, self.b, self.dist_func)
                    self.cost, self.path, self.C, self.P = self.full()
            # return and call full in async
            return self.cost

    def remove(self, a_remove, b_remove):
        # hit
        print("HIT")
        # frechet need to regenerate through path
        idx = self.path.index((a_remove, b_remove))
        print(self.path[idx:])
        self.C[a_remove + 1, b_remove + 1] = self.dist_matrix[a_remove, b_remove]
        for i in range(idx + 1, len(self.path)):
            x, y = self.path[i]
            prev_i, prev_j = self.path[i - 1]
            if self.dist_matrix[x, y] > self.C[prev_i + 1, prev_j + 1]:
                self.C[x + 1, y + 1] = self.dist_matrix[x, y]
            else:
                self.C[x + 1, y + 1] = self.C[prev_i + 1, prev_j + 1]
        n0, n1 = self.path[-1]
        self.cost = self.C[n0 + 1, n1 + 1]
        return self.cost

    def add(self, a_add: np.ndarray, b_add: np.ndarray):
        if a_add is not None:
            self.dist_matrix = np.vstack([self.dist_matrix, cdist(a_add, self.b)])
            self.a = np.vstack([self.a, a_add])
        if b_add is not None:
            self.dist_matrix = np.hstack([self.dist_matrix, cdist(self.a, b_add)])
            self.b = np.vstack([self.b, b_add])
        t0 = self.a
        t1 = self.b
        n0 = len(t0)
        n1 = len(t1)
        C = np.zeros((n0 + 1, n1 + 1))
        C[1:, 0] = float('inf')
        C[0, 1:] = float('inf')
        P = np.zeros((n0 + 1, n1 + 1, 2), dtype=int)
        ori0, ori1 = self.C.shape
        C[:ori0, :ori1] = self.C
        P[:ori0, :ori1] = self.P
        for i in np.arange(1, ori0):
            for j in np.arange(ori1, n1 + 1):
                candidates = [
                    (C[i - 1, j - 1], self.dist_matrix[i - 2, j - 2], (i - 1, j - 1)),
                    (C[i, j - 1], self.dist_matrix[i - 1, j - 2], (i, j - 1)),
                    (C[i - 1, j], self.dist_matrix[i - 2, j - 1], (i - 1, j)),
                ]
                candidates = sorted(candidates, key=operator.itemgetter(0, 1))
                C[i, j] = max(self.dist_matrix[i - 1, j - 1], candidates[0][0])
                P[i, j] = [candidates[0][2][0], candidates[0][2][1]]
        for i in np.arange(ori0, n1 + 1):
            for j in np.arange(1, n1 + 1):
                candidates = [
                    (C[i - 1, j - 1], self.dist_matrix[i - 2, j - 2], (i - 1, j - 1)),
                    (C[i, j - 1], self.dist_matrix[i - 1, j - 2], (i, j - 1)),
                    (C[i - 1, j], self.dist_matrix[i - 2, j - 1], (i - 1, j)),
                ]
                candidates = sorted(candidates, key=operator.itemgetter(0, 1))
                C[i, j] = max(self.dist_matrix[i - 1, j - 1], candidates[0][0])
                P[i, j] = [candidates[0][2][0], candidates[0][2][1]]
        dtw = C[n0, n1]
        pt = (n0, n1)
        path: list = [(pt[0] - 1, pt[1] - 1)]
        while pt != (1, 1):
            pt = tuple(P[pt[0], pt[1]])
            path.append((pt[0] - 1, pt[1] - 1))
        # path -= 1
        path.reverse()
        self.cost, self.path, self.C, self.P = dtw, path, C, P
        return dtw

    def full(self):
        t0 = self.a
        t1 = self.b
        n0 = len(t0)
        n1 = len(t1)
        C = np.zeros((n0 + 1, n1 + 1))
        P = np.zeros((n0 + 1, n1 + 1, 2), dtype=int)
        C[1:, 0] = float('inf')
        C[0, 1:] = float('inf')
        for i in np.arange(n0) + 1:
            for j in np.arange(n1) + 1:
                candidates = [
                    (C[i - 1, j - 1], self.dist_matrix[i - 2, j - 2], (i - 1, j - 1)),
                    (C[i, j - 1], self.dist_matrix[i - 1, j - 2], (i, j - 1)),
                    (C[i - 1, j], self.dist_matrix[i - 2, j - 1], (i - 1, j)),
                ]
                candidates = sorted(candidates, key=operator.itemgetter(0, 1, 2))
                C[i, j] = max(self.dist_matrix[i - 1, j - 1], candidates[0][0])
                P[i, j] = [candidates[0][2][0], candidates[0][2][1]]
        dtw = C[n0, n1]
        pt = (n0, n1)
        path: list = [(pt[0] - 1, pt[1] - 1)]
        while pt != (1, 1):
            pt = tuple(P[pt[0], pt[1]])
            path.append((pt[0] - 1, pt[1] - 1))
        path.reverse()
        return dtw, path, C, P


if __name__ == '__main__':
    a = np.asarray([[197],
                    [188],
                    [152],
                    [144],
                    [164]])
    b = np.asarray([[43],
                    [45],
                    [137],
                    [14],
                    [139]])

    truth = STFrechet(a[2:], b[2:], euclidean)
    our = STFrechet(a[:3], b[:3], euclidean)
    our.move(2, 2, a_add=a[3:], b_add=b[3:])
    print(truth.cost, our.cost)
    print(truth.path)