import redis
import unittest
import time

ONE_WEEK_IN_SECONDS = 7 * 86400
VOTE_SCORE = 432.0


def article_vote(conn, user, article):
    # 计算文章的投票截止时间
    cutoff = time.time() - ONE_WEEK_IN_SECONDS
    
    # 检查是否还能对文章进行投票
    if conn.zscore("time:", article) < cutoff:
        return 
    
    # 从article:id标识符（identifier）里面取出文章id
    article_id = article.split(":")[-1]

    # 如果用户是第一次为这篇文章投票，那么增加这篇文章的投票数量和评分
    if conn.sadd("voted:" + article_id, user):
        conn.zincrby(name="score:"+article, amount=VOTE_SCORE, value="test_user")
        conn.hincrby(article, "votes", 1)
    
    
def post_article(conn, user, title, link):
    # 生成一个新的文章id
    article_id = str(conn.incr("article:"))
    
    voted = "voted:" + article_id
    
    # 将发布文章的用户添加到文章的已投票用户名单中
    conn.sadd(voted, user)
    # 将这个名单的过期时间设置为一周
    conn.expire(voted, ONE_WEEK_IN_SECONDS)
    
    now = time.time()
    article = "article:" + article_id
    
    # 将文章信息存储到一个散列表里面
    conn.hmset(article, {
        "title": title,
        "link": link,
        "poster": user,
        "time": now,
        "votes": 1
    })
    print(now + VOTE_SCORE)
    # 将文章添加到根据评分排序的有序集合里面
    conn.zadd("score:", {article: now + VOTE_SCORE})
    # 将文章添加到根据发布时间排序的有序集合里面
    conn.zadd("time:", {article: now})
    
    return article_id


ARTICLES_PER_PAGE = 25


def get_articles(conn, page, order='score:'):
    # 设置获取文章的起始索引和结束索引
    start = (page-1) * ARTICLES_PER_PAGE
    end = start + ARTICLES_PER_PAGE - 1

    # 获取多个文章id
    ids = conn.zrevrange(order, start, end)
    print(ids)
    articles = []
    # 根据文章id获取文章详细信息
    for id in ids:
        article_data = conn.hgetall(id)
        article_data["id"] = id
        articles.append(article_data)

    return articles


def add_remove_groups(conn, article_id, to_add=[], to_remove=[]):
    # 构建存储文章信息的键名
    article = "article:" + article_id
    for group in to_add:
        # 将文章添加到它所属的群组里面
        conn.sadd("group:" + group, article)
    for group in to_remove:
        # 从群组里面移除文章
        conn.srem("group:" + group, article)


def get_group_articles(conn, group, page, order="score:"):
    # 为每个群组的每种拍了都创建一个键
    key = order + group
    # 检查是否已有缓存的排序结果，如果没有则排序
    if not conn.exists(key):
        # 根据评分或者发布时间，对群组文章进行排序
        conn.zinterstore(key, ["group:" + group, order], aggregate="max")
        conn.expire(key, 60)
    return get_articles(conn, page, key)


def del_all(conn):
    to_del = (
            conn.keys('time:*') + conn.keys('voted:*') + conn.keys('score:*') +
            conn.keys('article:*') + conn.keys('group:*')
    )
    if to_del:
        conn.delete(*to_del)


class Test01(unittest.TestCase):
    def setUp(self) -> None:
        pool = redis.ConnectionPool(host="localhost", port=6379, decode_responses=True)
        self.conn = redis.Redis(connection_pool=pool)
    
    def tearDown(self) -> None:
        del self.conn
    
    def test_postArticle(self):
        article_id = str(post_article(self.conn, "fle", "A title", "http://www.baidu.com"))
        print(article_id)
        r = self.conn.hgetall("article:" + article_id)
        print(r)

    def test_article_vote(self):
        article_id = str(5)
        article_vote(self.conn, 'test_user', 'article:'+article_id)
        v = int(self.conn.hget('article:' + article_id, 'votes'))
        print(v)

    def test_get_articles(self):
        articles = get_articles(self.conn, 1)
        print(articles)

    def test_add_remove_groups(self):
        article_id = str(4)
        add_remove_groups(self.conn, article_id, ["new_group"])
        articles = get_group_articles(self.conn, "new_group", 1)
        print(articles)

    def test_del_all(self):
        del_all(self.conn)
        self.test_get_articles()


if __name__ == "__main__":
    print(1)