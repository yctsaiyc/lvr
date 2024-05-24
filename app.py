import lvr
import glob


if __name__ == "__main__":
    lvr.save_history_season_raw_data()
    lvr.merge_csv_all_schemas()
