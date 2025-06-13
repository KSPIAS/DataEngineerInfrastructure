from etl.extract import extract_weather
from etl.transform import transform_weather
from etl.load import load_weather

province = "utt"
def main():
    df = extract_weather(city=province)
    if df.empty:
        print("No data extracted.")
        return

    df_transformed = transform_weather(df)
    load_weather(df_transformed)

if __name__ == "__main__":
    main()
