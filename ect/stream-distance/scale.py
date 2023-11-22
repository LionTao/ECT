import datetime
import math
from abc import abstractmethod
from typing import Optional, Callable, Tuple

import numpy as np
from dapr.actor import Actor
from dapr.actor import ActorInterface, actormethod
from dapr.actor.runtime._method_context import ActorMethodContext
from scipy.spatial.distance import cdist

rad = math.pi / 180.0
R = 6378137.0

from fastapi import FastAPI  # type: ignore
from dapr.actor.runtime.config import ActorRuntimeConfig, ActorTypeConfig, ActorReentrancyConfig
from dapr.actor.runtime.runtime import ActorRuntime
from dapr.ext.fastapi import DaprActor  # type: ignore
def great_circle_distance_gps(lon1, lat1, lon2, lat2):
    dlat = rad * (lat2 - lat1)
    dlon = rad * (lon2 - lon1)
    a = (math.sin(dlat / 2.0) * math.sin(dlat / 2.0) +
         math.cos(rad * lat1) * math.cos(rad * lat2) *
         math.sin(dlon / 2.0) * math.sin(dlon / 2.0))
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    d = R * c
    return d


def gpsdist(a, b):
    return great_circle_distance_gps(a[0], a[1], b[0], b[1])


class STDTW:
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
        self.cost = self.cost - (self.C[a_remove + 1, b_remove + 1] - self.dist_matrix[a_remove, b_remove])
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
                prev = np.argmin([C[i, j - 1], C[i - 1, j - 1], C[i - 1, j]])
                if prev == 0:
                    prev_i = i
                    prev_j = j - 1
                elif prev == 1:
                    prev_i = i - 1
                    prev_j = j - 1
                else:
                    prev_i = i - 1
                    prev_j = j
                C[i, j] = self.dist_matrix[i - 1, j - 1] + C[prev_i, prev_j]
                P[i, j] = [prev_i, prev_j]
        for i in np.arange(ori0, n1 + 1):
            for j in np.arange(1, n1 + 1):
                prev = np.argmin([C[i, j - 1], C[i - 1, j - 1], C[i - 1, j]])
                if prev == 0:
                    prev_i = i
                    prev_j = j - 1
                elif prev == 1:
                    prev_i = i - 1
                    prev_j = j - 1
                else:
                    prev_i = i - 1
                    prev_j = j
                C[i, j] = self.dist_matrix[i - 1, j - 1] + C[prev_i, prev_j]
                P[i, j] = [prev_i, prev_j]
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
                prev = np.argmin([C[i, j - 1], C[i - 1, j - 1], C[i - 1, j]])
                if prev == 0:
                    prev_i = i
                    prev_j = j - 1
                elif prev == 1:
                    prev_i = i - 1
                    prev_j = j - 1
                else:
                    prev_i = i - 1
                    prev_j = j
                C[i, j] = self.dist_matrix[i - 1, j - 1] + C[prev_i, prev_j]
                P[i, j] = [prev_i, prev_j]
        dtw = C[n0, n1]
        pt = (n0, n1)
        path: list = [(pt[0] - 1, pt[1] - 1)]
        while pt != (1, 1):
            pt = tuple(P[pt[0], pt[1]])
            path.append((pt[0] - 1, pt[1] - 1))
        path.reverse()
        return dtw, path, C, P


class DemoActorInterface(ActorInterface):

    @abstractmethod
    @actormethod(name="Init")
    async def init(self, data: dict) -> None:
        ...

    @abstractmethod
    @actormethod(name="Move")
    async def move(self, data: dict) -> float:
        ...

    @abstractmethod
    @actormethod(name="Reset")
    async def reset(self) -> None:
        ...


class DemoActor(Actor, DemoActorInterface):
    """Implements DemoActor actor service
    This shows the usage of the below actor features:
    1. Actor method invocation
    2. Actor state store management
    3. Actor reminder
    4. Actor timer
    """

    def __init__(self, ctx, actor_id):
        super(DemoActor, self).__init__(ctx, actor_id)
        self.our: STDTW = None

    async def _on_activate(self) -> None:
        """An callback which will be called whenever actor is activated."""
        print(f'Activate {self.__class__.__name__} actor!', flush=True)

    async def _on_deactivate(self) -> None:
        """An callback which will be called whenever actor is deactivated."""
        print(f'Deactivate {self.__class__.__name__} actor!', flush=True)

    async def _on_post_actor_method(self, method_context: ActorMethodContext) -> None:
        if method_context.method_name=="Move":
            self.our.full()


    async def init(self, data: dict) -> float:
        self.our = STDTW(np.asarray(data["a"]), np.asarray(data["b"]), gpsdist)
        return self.our.cost

    async def move(self, data: dict) -> float:
        return self.our.move(**data)

    async def reset(self) -> None:
        self.our.full()

app = FastAPI(title=f'{DemoActor.__name__}Service')

# This is an optional advanced configuration which enables reentrancy only for the
# specified actor type. By default reentrancy is not enabled for all actor types.
config = ActorRuntimeConfig()  # init with default values
config.update_actor_type_configs([
    ActorTypeConfig(
        actor_type=DemoActor.__name__,
        reentrancy=ActorReentrancyConfig(enabled=True))
])
ActorRuntime.set_actor_config(config)

# Add Dapr Actor Extension
actor = DaprActor(app)


@app.on_event("startup")
async def startup_event():
    # Register DemoActor
    await actor.register_actor(DemoActor)