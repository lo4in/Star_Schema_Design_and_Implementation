import pandas as pd
import json
import ast

def check_json_and_report(row):
    try:
        json.loads(row)
        return True
    except json.JSONDecodeError as e:
        print("\nError in line")
        print(row[:300])
        print(f"details {e}")
        return False
def parse_python_list(row):
    try:
        return ast.literal_eval(row)  
    except Exception as e:
        print("Error", e)
        print("line:", row[:200])
        return None


def smart_parse(x):
    if isinstance(x, str):
        try:
            
            return json.loads(x)
        except json.JSONDecodeError:
            try:
                
                return ast.literal_eval(x)
            except Exception as e:
                print("error", e)
                print("line", x[:100])
                return None
    elif isinstance(x, list):
        return x
    else:
        return None


df = pd.read_excel("MeetingReport/raw_data.xlsx")

parsed = df["raw_content"].apply(json.loads)


df["raw_id"] = parsed.apply(lambda x: x.get("id"))
df["raw_title"] = parsed.apply(lambda x: x.get("title"))
df["raw_duration"] = parsed.apply(lambda x: x.get("duration"))
df["calendar_id"] = parsed.apply(lambda x: x.get("calendar_id"))
df["transcript_url"] = parsed.apply(lambda x: x.get("transcript_url"))
df["audio_url"] = parsed.apply(lambda x: x.get("audio_url"))
df["video_url"] = parsed.apply(lambda x: x.get("video_url"))
df["dateString"] = parsed.apply(lambda x: x.get("dateString"))
df["host_email"] = parsed.apply(lambda x: x.get("host_email"))
df["organizer_email"] = parsed.apply(lambda x: x.get("organizer_email"))
df["participants"] = parsed.apply(lambda x: x.get("participants"))
df["speakers"] = parsed.apply(lambda x: x.get("speakers"))
df["meeting_attendees"] = parsed.apply(lambda x: x.get("meeting_attendees"))






fact_data = df.drop(columns = ['raw_content', 'participants','speakers', 'meeting_attendees'])

name_data = pd.DataFrame(df['speakers'].astype(str))




name_data["speakers"] = name_data["speakers"].apply(parse_python_list)

parsed1 = name_data["speakers"].apply(smart_parse)




speaker_names = parsed1.apply(lambda lst: [d.get("name") for d in lst] if isinstance(lst, list) else [])


df_speakers_flat = pd.DataFrame({"speaker_name": speaker_names.explode().dropna().reset_index(drop=True)})


dim_speaker = df_speakers_flat.drop_duplicates().reset_index(drop=True)


dim_speaker["speaker_id"] = dim_speaker.index + 1







user_rows = []


for i, row in df.iterrows():
    comm_id = row["id"]
    attendees = row["meeting_attendees"]
    if isinstance(attendees, list):
        for person in attendees:
            user_rows.append({
                "email": person.get("email"),
                "name": person.get("name"),
                "location": person.get("location"),
                "displayName": person.get("displayName"),
                "phoneNumber": person.get("phoneNumber")
            })


dim_user = pd.DataFrame(user_rows)


dim_user = dim_user.drop_duplicates(subset=["email"]).reset_index(drop=True)



dim_user['displayName'] = dim_speaker['speaker_name']
dim_user['user_id'] = df['raw_id']
dim_user = dim_user.drop(columns =['name', 'location', 'phoneNumber'])
dim_user = dim_user.dropna()




dim_comm_type = pd.DataFrame()
dim_comm_type['comm_type'] = df['comm_type']
dim_comm_type = dim_comm_type.drop_duplicates()
dim_comm_type["comm_type_id"] = dim_comm_type.index + 1


dim_subject = pd.DataFrame()
dim_subject['subject'] = fact_data['subject']
dim_subject['subject_id']= dim_subject.index+1

dim_calendar = pd.DataFrame()
dim_calendar['raw_calendar_id'] = df['calendar_id']
dim_calendar["calendar_id"] = dim_calendar.index + 1

dim_audio = pd.DataFrame()
dim_audio['raw_audio_url'] = df['audio_url'] 
dim_audio["audio_id"] = dim_audio.index + 1

dim_video = pd.DataFrame()
dim_video['raw_video_url'] = df['video_url']
dim_video["video_id"] = dim_video.index + 1

dim_transcript = pd.DataFrame()
dim_transcript['raw_transcript_url'] = df['transcript_url']
dim_transcript["transcript_id"] = dim_transcript.index + 1


#Dim-Fact-Communication created:
dim_fact_communication = pd.DataFrame()

dim_fact_communication['comm_id'] = fact_data['id']
dim_fact_communication['raw_id'] = fact_data['raw_id']
dim_fact_communication['source_id'] = fact_data['source_id']
dim_fact_communication["comm_type_id"] = dim_comm_type['comm_type_id']
dim_fact_communication["subject_id"] = dim_subject['subject_id']
dim_fact_communication["calendar_id"] = dim_calendar['calendar_id']
dim_fact_communication["video_id"] = dim_video['video_id']
dim_fact_communication['transcript_id'] = dim_transcript['transcript_id']
dim_fact_communication['date_time_id'] = fact_data['dateString']
dim_fact_communication['ingested_at'] = fact_data['ingested_at']
dim_fact_communication['processed_at'] = fact_data['processed_at']
dim_fact_communication['is_processed'] = fact_data['is_processed']
dim_fact_communication['raw_title'] = fact_data['raw_title']
dim_fact_communication['raw_duration'] = fact_data['raw_duration']


rows = []

for i, row in df.iterrows():
    comm_id = row["id"]
    duration = row["raw_duration"]
    attendees = row["meeting_attendees"]

    if isinstance(attendees, list):
        for person in attendees:
            rows.append({
                "comm_id": comm_id,
                "raw_duration": duration,
                "email": person.get("email"),
                "isAttendee": person.get("isAttendee", False),
                "isParticipant": person.get("isParticipant", False),
                "isSpeaker": person.get("isSpeaker", False),
                "isOrganiser": person.get("isOrganiser", False)
            })

bridge_df = pd.DataFrame(rows)
bridge_df['email'] = dim_user['user_id']
bridge_df = bridge_df.drop(columns=['raw_duration'])
bridge_df = bridge_df.dropna()



df_list = {name: val for name, val in globals().items() if isinstance(val, pd.DataFrame)}


print(list(df_list.keys()))


print(f'dim_speaker\n', dim_speaker)
print(f'dim_user\n', dim_user)
print(f'dim_comm_type\n', dim_comm_type)
print(f'dim_subject\n', dim_subject)
print(f'dim_calendar\n', dim_calendar)
print(f'dim_audio\n', dim_audio)
print(f'dim_video\n', dim_video)
print(f'dim_transcript\n', dim_transcript)
print(f'dim_fact_communication\n', dim_fact_communication)
print(f'bridge_df\n', bridge_df)






with pd.ExcelWriter("star_schema_output_final.xlsx") as writer:
    dim_speaker.to_excel(writer, sheet_name="dim_speaker", index=False)
    dim_user.to_excel(writer, sheet_name="dim_user", index=False)
    dim_comm_type.to_excel(writer, sheet_name="dim_comm_type", index=False)
    dim_subject.to_excel(writer, sheet_name="dim_subject", index=False)
    dim_calendar.to_excel(writer, sheet_name="dim_calendar", index=False)
    dim_audio.to_excel(writer, sheet_name="dim_audio", index=False)
    dim_video.to_excel(writer, sheet_name="dim_video", index=False)
    dim_transcript.to_excel(writer, sheet_name="dim_transcript", index=False)
    dim_fact_communication.to_excel(writer, sheet_name="fact_communication", index=False)
    bridge_df.to_excel(writer, sheet_name="bridge_comm_user", index=False)