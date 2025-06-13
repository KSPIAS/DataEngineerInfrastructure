import pandas as pd

def transform_weather(df: pd.DataFrame) -> pd.DataFrame:
    # แปลง observation_time ให้เป็น datetime
    df["observation_time"] = pd.to_datetime(df["observation_time"])
    
    # ตัวอย่าง: เพิ่มคอลัมน์อุณหภูมิเป็นฟาเรนไฮต์
    df["temperature_f"] = df["temperature_c"] * 9/5 + 32

    # ลบข้อมูลซ้ำ (ถ้ามี)
    df = df.drop_duplicates(subset=["city", "observation_time"])

    return df
