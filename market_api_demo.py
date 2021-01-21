#!/usr/bin/python3
"""
Pharbers SideWay Pods Metrics Demo
"""

import datetime
import os
import requests
import redis
import time
import json
from six import string_types
from six.moves.urllib.parse import urlencode, urlunparse  # noqa


app_id = "1641099262856206"
app_secret = "31c870abbe108f05fe70828977ed23393b945e77"
auth_code = "66e6c087f2eed3dd55872801918bd0fe7e3374fb"
advertiser_name = "普菲特海南"

def get_access_token():
	open_api_url_prefix = "https://ad.oceanengine.com/open_api/"
	uri = "oauth2/access_token/"
	url = open_api_url_prefix + uri
	data = {
		"app_id": app_id,
		"secret": app_secret,
		"grant_type": "auth_code",
		"auth_code": auth_code
	}
	rsp = requests.post(url, json=data)
	rsp_data = rsp.json()
	return rsp_data
	

def query_market_api_token():
	r = redis.StrictRedis(host='pharbers-cache.xtjxgq.0001.cnw1.cache.amazonaws.com.cn', port=6379, db=0, decode_responses=True)
	v = r.hgetall(advertiser_name)
	if v:
		# r.delete(advertiser_name)
		return v["access_token"]
	else:
		m = get_access_token()
		# m = get_access_token_fork()
		d = m["data"]
		k = d["advertiser_name"]
		e = d["expires_in"]
		del d["advertiser_ids"]
		r.hset(k, mapping=d)
		r.expire(k, e)
		return d["access_token"]


def query_market_api_advertiser_id():
	r = redis.StrictRedis(host='pharbers-cache.xtjxgq.0001.cnw1.cache.amazonaws.com.cn', port=6379, db=0, decode_responses=True)
	v = r.hgetall(advertiser_name)
	if v:
		# r.delete(advertiser_name)
		return v["advertiser_id"]
	else:
		m = get_access_token()
		# m = get_access_token_fork()
		d = m["data"]
		k = d["advertiser_name"]
		e = d["expires_in"]
		del d["advertiser_ids"]
		r.hset(k, mapping=d)
		r.expire(k, e)
		return d["advertiser_id"]
 

"""
	其中能得到的数据包括
	"live_watch_count",  # 观看数
	"live_watch_ucount",  # 观看人数
	"live_watch_duration",  # 观看时长
	"pay_order_gmv",  # 商品订单金额
	"live_count",  # 直播场次
	"live_avg_watch_count",  # 场均观看次数
	"live_avg_watch_duration",  # 人均停留时长
	"live_pay_avg_order_gmv",  # 场均订单金额
	"live_duration_60s_count",  # 超1分钟观看数
	"live_duration_60s_ucount",  # 超1分钟观看人数
	"live_online_ucount",  # 直播间内人数
	"click_product_count",  # 商品点击数
	"click_product_ucount",  # 商品点击人数
	"live_orders_count",  # 商品下单数
	"order_ucount",  # 商品下单人数
	"pay_order_count",  # 商品订单数
	"pay_order_ucount",  # 商品订单人数
	"order_convert_rate",  # 订单转化率
	"per_customer_transaction",  # 客单价
	"live_follow_count",  # 关注数
	"live_fans_count",  # 加入粉丝团数
	"live_comment_count",  # 评论数
	"live_share_count",  # 分享数
	"live_gift_count",  # 打赏次数
	"live_gift_money",  # 礼物总金额
	"click_cart_count",  # 查看购物车数
	"live_duration_60s_count_to_live_watch_count_rate",  # 转化率（观看数-超1分钟观看数）
	"click_product_count_to_live_duration_60s_count_rate",  # 转化率（超1分钟观看数-商品点击数）
	"live_orders_count_to_click_product_count_rate",  # 转化率（商品点击数-商品下单数）
	"pay_order_count_to_live_orders_count_rate",  # 转化率（商品下单数-商品订单数）
	"live_duration_60s_ucount_to_live_watch_ucount_rate",  # 转化率（观看人数-超1分钟观看人数）
	"click_product_ucount_to_live_duration_60s_ucount_rate",  # 转化率（超1分钟观看人数-商品点击人数）
	"order_ucount_to_click_product_ucount_rate",  # 转化率（商品点击人数-商品下单人数）
	"pay_order_ucount_to_order_ucount_rate",  # 转化率（商品下单人数-商品订单人数）
"""
def query_live_analysis_by_advertiser_id(json_str):
	args = json.loads(json_str)
	PATH = "/report/live_room/analysis/get"
	query_string = urlencode({k: v if isinstance(v, string_types) else json.dumps(v) for k, v in args.items()})
	url = urlunparse(("https","ad.oceanengine.com/open_api/2", PATH, "", query_string, ""))
	headers = {
		"Access-Token": query_market_api_token(),
	}
	rsp = requests.get(url, headers=headers)
	return rsp.json()


if __name__ == "__main__":

	# 1. 得到token
	token = query_market_api_token()
	print(token)

	# 2. 得到直播间分析报表数据
	end_time = datetime.datetime.now()
	start_time = end_time + datetime.timedelta(minutes=-5)
	print(end_time.strftime("%Y-%m-%d %H:%M:%S"))
	print(start_time.strftime("%Y-%m-%d %H:%M:%S"))
	args = {}
	args["advertiser_id"] = query_market_api_advertiser_id()
	# args["advertiser_id"] = "adfasdfj"
	args["end_time"] = end_time.strftime("%Y-%m-%d %H:%M:%S")
	args["start_time"] = start_time.strftime("%Y-%m-%d %H:%M:%S")
	# my_args = "{\"advertiser_id\": ADVERTISER_ID,\"end_time\": \"END_TIME\",\"fields\": [\"FIELDS\"],\"filtering\": {\"product_ids\": [\"PRODUCT_IDS\"],\"room_ids\": [\"ROOM_IDS\"]},\"order_field\": \"ORDER_FIELD\",\"order_type\": \"ORDER_TYPE\",\"page\": PAGE,\"page_size\": PAGE_SIZE,\"start_time\": \"START_TIME\"}"
	res = query_live_analysis_by_advertiser_id(json.dumps(args))
	print(res)
