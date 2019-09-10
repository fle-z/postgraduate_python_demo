import redis
import time
from urllib import parse
import unittest
import uuid
import threading


def check_token(conn, token):
    # 尝试获取并返回令牌对应的用户
    return conn.hget("login:", token)


def update_token(conn, token, user, item=None):
    # 获取当前时间戳
    timestamp = time.time()
    # 维持令牌与已登录用户的映射
    conn.hset("login:", token, user)
    # 记录令牌最后一次出现的时间
    conn.zadd("recent:", {token: timestamp})
    if item:
        # 记录用户浏览过的商品
        conn.zadd("viewed:" + token, {item: timestamp})
        # 移除旧的记录，只保留用户最近浏览过的25个商品
        conn.zremrangebyrank("viewed:" + token, 0, -26)


QUIT = False
LIMIT = 1000000


def clean_sessions(conn):
    while not QUIT:
        # 找出目前已有令牌的数量
        size = conn.zcard("recent:")
        # 若令牌数量未超过限制，休眠，并在之后重新检查
        if size <= LIMIT:
            time.sleep(1)
            continue
    # 获取需要移除的令牌id
    end_index = min(size - LIMIT, 100)
    tokens = conn.zrange("recent:", 0, end_index-1)

    # 为那些要被删除的令牌构建键名
    session_keys = []
    for token in tokens:
        session_keys.append("viewed:" + token)
        # 新增一行：删除旧会话对应用户的购物车
        session_keys.append(("cart:" + token))

    # 移除最旧的令牌
    conn.delete(*session_keys)
    conn.hdel("login:", *tokens)
    conn.zrem("recent:", *tokens)


def add_to_card(conn, session, item, count):
    if count <= 0:
        # 从购物车里移除指定商品
        conn.hrem("cart:" + session, item)
    else:
        # 将指定的商品添加到购物车
        conn.hset("cart:" + session, item, count)


def clean_full_sessions(conn):
    while not QUIT:
        size = conn.zcard("recent:")
        if size <= LIMIT:
            time.sleep(1)
            continue

    # 获取需要移除的令牌id
    end_index = min(size - LIMIT, 100)
    tokens = conn.zrange("recent:", 0, end_index-1)

    # 为那些要被删除的令牌构建键名
    session_keys = []
    for token in tokens:
        session_keys.append("viewed:" + token)

    # 移除最旧的令牌


# --------------- 以下是用于测试代码的辅助函数 --------------------------------
def extract_item_id(request):
    parsed = parse.urlparse(request)
    query = parse.parse_qs(parsed.query)
    return (query.get("item") or [None])[0]


def is_dynamic(request):
    parsed = parse.urlparse(request)
    query = parse.parse_qs(parsed.query)
    return '_' in query


def hash_request(request):
    return str(hash(request))


class Inventory(object):
    def __init__(self, id):
        self.id = id

    @classmethod
    def get(cls, id):
        return Inventory(id)

    def to_dict(self):
        return {"id": self.id, "data": "data to cache...", "cached": time.time()}


class Test02(unittest.TestCase):
    def setUp(self) -> None:
        pool = redis.ConnectionPool(host="localhost", port=6379, decode_responses=True)
        self.conn = redis.Redis(connection_pool=pool)

    def tearDown(self) -> None:
        conn = self.conn
        to_del = (
                conn.keys('login:*') + conn.keys('recent:*') + conn.keys('viewed:*') +
                conn.keys('cart:*') + conn.keys('cache:*') + conn.keys('delay:*') +
                conn.keys('schedule:*') + conn.keys('inv:*'))
        if to_del:
            self.conn.delete(*to_del)
        del self.conn
        global QUIT, LIMIT
        QUIT = False
        LIMIT = 10000000

    def test_login_cookies(self):
        conn = self.conn
        global LIMIT, QUIT
        token = str(uuid.uuid4())

        update_token(conn, token, "username", "itemX")
        print(token)
        r = check_token(conn, token)
        print(r)

        # 将存储cookies的最大值设为0，清空cookies
        LIMIT = 0
        t = threading.Thread(target=clean_sessions, args=(conn,))
        t.setDaemon(1)
        t.start()
        time.sleep(1)
        QUIT = True
        time.sleep(2)
        if t.is_alive():
            raise Exception("The clean sessions thread is still alive!")

        s = conn.hlen("login:")
        print(s)

    def test_shopping_cart_cookies(self):
        conn = self.conn
        global LIMIT, QUIT
        token = str(uuid.uuid4())

        update_token(conn, token, "username", "itemX")
        print(token)
        r = check_token(conn, token)
        print(r)

        # 将存储cookies的最大值设为0，清空cookies
        LIMIT = 0
        t = threading.Thread(target=clean_sessions, args=(conn,))
        t.setDaemon(1)
        t.start()
        time.sleep(1)
        QUIT = True
        time.sleep(2)
        if t.is_alive():
            raise Exception("The clean sessions thread is still alive!")

