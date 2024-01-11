import requests

def get_video_content(video_id: str, api_key: str):
    target_url = f"https://youtube.googleapis.com/youtube/v3/commentThreads?part=snippet&part=replies&videoId={video_id}&key={api_key}&alt=json"
    video_request = requests.get(target_url)
    if video_request.status_code == 200:
        #TODO entender como pegar as próximas páginas a partir do token
        return requests.get(target_url).json()["items"]
    return "Video fora do ar!"




if __name__ == "__main__":
    from decouple import config
    my_api_key = config("API_KEY")

video = get_video_content(video_id="q6yOF3nkevY", api_key=my_api_key)
print(video)