import subprocess

# curl_command = 'curl -H "Host: lmscript.xyz" -H "content-type: application/x-www-form-urlencoded" -H "user-agent: okhttp/4.9.0" --data-binary "username=ccddd2%40gmail.com&password=XpF8bxvLKU**" --compressed "https://lmscript.xyz/v1/auth/login"'
# curl_command = 'curl -H "Host: lmscript.xyz" -H "authorization: Bearer ZHEeCepPS2n3T-7nyYwfNFBQqC8Fhr_v" -H "user-agent: okhttp/4.9.0" --compressed "https://lmscript.xyz/v1/movies/index?sort=-date_added&page=1"'
# curl_command = 'curl -H "Host: lmscript.xyz" -H "authorization: Bearer ZHEeCepPS2n3T-7nyYwfNFBQqC8Fhr_v" -H "user-agent: okhttp/4.9.0" --compressed "https://lmscript.xyz/v1/shows?sort=-date_added&page=1"'
# curl_command = 'curl -H "Host: lmscript.xyz" -H "authorization: Bearer ZHEeCepPS2n3T-7nyYwfNFBQqC8Fhr_v" -H "user-agent: okhttp/4.9.0" --compressed "https://lmscript.xyz/v1/shows?expand=subtitles,streams,episodes,suggestions&id=6875"'
# curl_command = 'curl -H "Host: lmscript.xyz" -H "authorization: Bearer ZHEeCepPS2n3T-7nyYwfNFBQqC8Fhr_v" -H "user-agent: okhttp/4.9.0" --compressed "https://lmscript.xyz/v1/episodes?expand=subtitles,streams,episodes,suggestions&id=229680"'
# curl_command = 'curl -H "Host: lmscript.xyz" -H "authorization: Bearer ZHEeCepPS2n3T-7nyYwfNFBQqC8Fhr_v" -H "user-agent: okhttp/4.9.0" --compressed "https://lmscript.xyz/v1/episodes?sort=-air_date&page=1"'
curl_command = 'curl -H "Host: lmscript.xyz" -H "authorization: Bearer ZHEeCepPS2n3T-7nyYwfNFBQqC8Fhr_v" -H "user-agent: okhttp/4.9.0" --compressed "https://lmscript.xyz/v1/movies/view?expand=subtitles,streams,suggestions&id=135906"'
# curl_command = 'curl -H "Host: lmscript.xyz" -H "authorization: Bearer ZHEeCepPS2n3T-7nyYwfNFBQqC8Fhr_v" -H "user-agent: okhttp/4.9.0" --compressed "https://lmscript.xyz/v1/streaming-servers"'
result = subprocess.run(curl_command,shell=True,capture_output=True,text=True)
with open('test.json', 'w', encoding='utf-8') as f:
    f.write(result.stdout)

