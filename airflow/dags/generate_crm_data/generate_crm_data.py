from datetime import datetime, timedelta
import random
import pandas as pd
import numpy as np

def generate_users(start_id, day_offset, existing_user_ids):
    total_days = 60
    base_date = datetime.now().date() + timedelta(days=day_offset)

    # Рост регистраций: от 4 до 8 в день с колебаниями
    linear_trend = np.linspace(4, 8, total_days)
    sin_wave = 1.5 * np.sin(np.linspace(0, 4 * np.pi, total_days))
    daily_count = max(1, int(linear_trend[day_offset % total_days] + sin_wave[day_offset % total_days]))

    data = []
    user_id = start_id

    for _ in range(daily_count):
        timestamp = datetime(
            base_date.year,
            base_date.month,
            base_date.day,
            random.randint(8, 20),
            random.randint(0, 59),
            random.randint(0, 59),
        )
        age = random.randint(18, 50)
        city_id = random.choices([1, 2, 3, 4, 5], weights=[3, 3, 1, 1, 1])[0]
        source_id = random.choices([1, 2, 3, 4, 5], weights=[3, 3, 1, 1, 1])[0]

        if user_id in existing_user_ids:
            user_id += 1
            continue

        data.append([user_id, timestamp, age, city_id, source_id])
        user_id += 1

    return pd.DataFrame(data, columns=["id", "registered_at", "age", "city_id", "source_id"])


def generate_enrollments(users_df, start_id, day_offset,
                         existing_enroll_ids=None, enrollments_history_df=None,
                         course_ids=[1, 2, 3]):
    if existing_enroll_ids is None:
        existing_enroll_ids = set()

    if enrollments_history_df is None:
        enrollments_history_df = pd.DataFrame(columns=["user_id", "course_id", "enrolled_at"])

    total_days = 60
    statuses = ["paid", "free_trial", "unpaid"]
    enrollments = []
    enroll_id = start_id

    for _, row in users_df.iterrows():
        user_id = row['id']
        registered_at = row['registered_at']

        base_ratio = 0.5 + (day_offset / total_days) * 0.3
        enroll_chance = base_ratio + random.uniform(-0.1, 0.1)

        if random.random() < enroll_chance:
            past_courses = set(
                enrollments_history_df[
                    (enrollments_history_df['user_id'] == user_id) &
                    (enrollments_history_df['enrolled_at'] < registered_at)
                ]['course_id']
            )

            available_courses = [c for c in course_ids if c not in past_courses]
            if not available_courses:
                continue

            course_id = random.choice(available_courses)
            enrolled_at = registered_at + timedelta(minutes=random.randint(10, 300))
            payment_status = random.choice(statuses)

            while enroll_id in existing_enroll_ids:
                enroll_id += 1

            enrollments.append({
                "id": enroll_id,
                "user_id": user_id,
                "course_id": course_id,
                "enrolled_at": enrolled_at,
                "payment_status": payment_status,
            })
            existing_enroll_ids.add(enroll_id)
            enroll_id += 1

    return pd.DataFrame(enrollments)