import platform
import re
import aiohttp
import m3u8
import asyncio

if platform.system() == 'Windows':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

url = "https://www.lookmovie2.to/movies/view/9150192-a-working-man-2025"
headers = {
    'accept': '*/*',
    'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6,en-GB-oxendict;q=0.5,zh-TW;q=0.4,ja;q=0.3',
    'cache-control': 'no-cache',
    'pragma': 'no-cache',
    'priority': 'u=1, i',
    'referer': 'https://www.lookmovie2.to/movies/play/9150192-a-working-man-2025',
    'sec-ch-ua': '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-origin',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36',
    'x-requested-with': 'XMLHttpRequest',
    #'cookie': 'b-user-id=2e367707-d1aa-b7ed-8cb8-c5b919e99c3f; _ym_uid=174338829293755151; _ym_d=1743406819; PHPSESSID=rjspufpo8fi2rm5s7dogrir29i; _csrf=ad63dcd18e6c4238a0ed25854adcc35c9087f66aab6e131a7baebb4f647b4055a%3A2%3A%7Bi%3A0%3Bs%3A5%3A%22_csrf%22%3Bi%3A1%3Bs%3A32%3A%22eKI8rpYGmwH67rgZqhNR_ZhSGYCdegvv%22%3B%7D; _clck=1j65b1c%7C2%7Cfv3%7C0%7C1916; _ym_isad=2; _clsk=ddfd3v%7C1744706385652%7C3%7C0%7Cp.clarity.ms%2Fcollect',
}
headers = {
        "accept": "*/*",
        "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
        "cache-control": "no-cache",
        "pragma": "no-cache",
        "sec-ch-ua": "\"Chromium\";v=\"134\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\"",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
        "x-requested-with": "XMLHttpRequest"
    }
cookies = {
        "b-user-id": "2e367707-d1aa-b7ed-8cb8-c5b919e99c3f",
        "_ym_uid": "174338829293755151",
        "_ym_d": "1743406819",
        "lm-become-premium-closed": "true",
        "PHPSESSID": "rjspufpo8fi2rm5s7dogrir29i",
        "_csrf": "170adceecdc6158e14a32cd0fce70a8a1f6cc8910d1e8836328ca4bc3902409ea%3A2%3A%7Bi%3A0%3Bs%3A5%3A%22_csrf%22%3Bi%3A1%3Bs%3A32%3A%22mSbcSaVHK4wTi3M-tOdGKiE4JyjbEB4W%22%3B%7D",
        "_clck": "1j65b1c%7C2%7Cfuq%7C0%7C1916",
        "_ym_isad": "2"
}
# 定义一个异步函数download
async def get_paly_url(url):
    play_url = url.replace('/view/', '/play/')
    async with aiohttp.ClientSession(headers=headers,cookies=cookies) as session:
        async with session.get(url=play_url) as resp:
            content = await resp.text()
            hash_value = re.findall(r'hash: "(.*?)",', content, re.S)[0]
            expires = re.findall(r"expires: (.*?)}", content, re.S)[0].strip()
            id_movie = re.findall(r"id_movie: (.*?),", content, re.S)[0]
            return f'https://www.lookmovie2.to/api/v1/security/movie-access?id_movie={id_movie}&hash={hash_value}&expires={expires}'
async def download(url):
    # 使用aiohttp库创建一个异步的ClientSession对象
    async with aiohttp.ClientSession(headers=headers,cookies=cookies) as session:
        # 使用session对象发送一个异步的GET请求
        async with session.get(url=url) as resp:
            # 等待响应内容
            content = await resp.json()
            # print(content)
        async with session.get(url=content['streams']['720p']) as resp:
            content = await resp.text()
            # 返回解析后的m3u8内容
            return m3u8.loads(content)

async def main(url=url):
    play_url = await get_paly_url(url)
    m3u8_obj = await download(play_url)

    for i, segment in enumerate(m3u8_obj.segments):
        print(f'{i} {segment.uri}')
        if segment.key:
            print("有加密")

asyncio.run(main())