#!/usr/bin/python3

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