# EcuadorCrisis_TikTok


This repository contains TikTok videos related to the Ecuador political crisis that occurred on January 2024. 

### Organization 
- /scripts/collect_metadata.py: TikTok Research API metadata collection script
- /scripts/collect_videos.py: PykTok application for scraping videos (Video ID is a required to run this script)
- /supplemental_materials/keywords_hashtags.txt: Text file of keywords and hastags used within the (video metadata) query
- /data: CSV file that contains the video id ('id') and its associated Whisper Generate AI transcript ('whisper_voice_to_text')