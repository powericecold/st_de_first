from pymongo import MongoClient
from datetime import datetime, timedelta
import json


def archive_inactive_users():
    # Подключение к MongoDB
    client = MongoClient("mongodb://localhost:27017/")
    db = client["my_database"]
    user_events = db["user_events"]
    archived_users = db["archived_users"]

    # Текущая дата (начало дня)
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)

    # Рассчет пороговых дат
    registration_cutoff = today - timedelta(days=30)
    activity_cutoff = today - timedelta(days=14)

    # Агрегация для поиска неактивных пользователей
    pipeline = [
        {
            "$group": {
                "_id": "$user_id",
                "registration_date": {"$first": "$user_info.registration_date"},
                "last_activity": {"$max": "$event_time"}
            }
        },
        {
            "$match": {
                "registration_date": {"$lt": registration_cutoff},
                "last_activity": {"$lt": activity_cutoff}
            }
        },
        {"$project": {"_id": 0, "user_id": "$_id"}}
    ]

    # Получение списка пользователей для архивации
    inactive_users = list(user_events.aggregate(pipeline))
    user_ids = [user["user_id"] for user in inactive_users]

    # Перемещение пользователей в архив
    if user_ids:
        events_to_archive = user_events.find({"user_id": {"$in": user_ids}})
        archived_users.insert_many(events_to_archive)

        user_events.delete_many({"user_id": {"$in": user_ids}})

    # Формирование отчета
    report = {
        "date": today.strftime("%Y-%m-%d"),
        "archived_users_count": len(user_ids),
        "archived_user_ids": sorted(user_ids)
    }

    filename = f"{today.strftime('%Y-%m-%d')}.json"
    with open(filename, 'w') as f:
        json.dump(report, f, indent=2)

    print(f"✅ Архивация завершена. Перемещено пользователей: {len(user_ids)}")


if __name__ == "__main__":
    archive_inactive_users()