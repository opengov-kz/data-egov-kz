# main.py (Fixed Version)
import json
import logging
import time
import concurrent.futures
from config import BASE_URL, CGO_DATASOURCE, MIO_DATASOURCE, QUASIORG_DATASOURCE
from utils.selenium_utils import SeleniumManager, process_agency_datasets

# Configure logging
logging.basicConfig(
    filename="logs/error.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


def load_gov_agencies(json_file):
    """Load government agencies from JSON file"""
    with open(json_file, 'r', encoding='utf-8') as file:
        data = json.load(file)
    return [item['govAgency'] for item in data]


def main():
    """Main execution function"""
    start_time = time.time()
    manager = SeleniumManager(max_workers=3)  # Reduced parallelism for stability

    try:
        # Configure output files
        datasets = {
            CGO_DATASOURCE: "data/byCGO.csv",
            MIO_DATASOURCE: "data/byMIO.csv",
            QUASIORG_DATASOURCE: "data/byQuasiOrg.csv"
        }

        # Process each dataset file sequentially (better for error handling)
        for json_file, output_csv in datasets.items():
            gov_agencies = load_gov_agencies(json_file)
            print(f"\nFound {len(gov_agencies)} agencies in {json_file}")

            # Process agencies with limited parallelism
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                futures = []

                for agency in gov_agencies:
                    futures.append(
                        executor.submit(
                            process_agency_datasets,
                            manager=manager,
                            agency_id=agency,
                            output_csv=output_csv,
                            pages=5  # Process first 5 pages per agency
                        )
                    )
                    time.sleep(1)  # Stagger startup

                # Monitor progress
                for i, future in enumerate(concurrent.futures.as_completed(futures)):
                    try:
                        result = future.result()
                        print(f"Completed agency {i + 1}/{len(gov_agencies)}")
                    except Exception as e:
                        logging.error(f"Agency processing failed: {e}")

    except Exception as e:
        logging.error(f"Fatal error in main: {e}")
    finally:
        manager.cleanup()
        elapsed = (time.time() - start_time) / 60
        print(f"\nTotal execution time: {elapsed:.2f} minutes")
        print("All processing completed")


if __name__ == "__main__":
    main()