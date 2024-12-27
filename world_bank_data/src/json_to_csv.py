import json
import pandas as pd
from pathlib import Path
import logging
from typing import Dict, List, Union

class JsonToCsvConverter:
    def __init__(self):
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def _handle_world_bank_response(self, data: List) -> pd.DataFrame:
        """Handle World Bank API specific response format [metadata, data]"""
        if isinstance(data, list) and len(data) == 2:
            # Second element contains the actual data
            if isinstance(data[1], list):
                return pd.DataFrame(data[1])
            else:
                return pd.DataFrame([data[1]])
        return pd.DataFrame(data)

    def _flatten_dict(self, d: Dict, parent_key: str = '', sep: str = '_') -> Dict:
        """Flatten a dictionary with nested structures"""
        items: List = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(self._flatten_dict(v, new_key, sep=sep).items())
            elif isinstance(v, list):
                if v and isinstance(v[0], dict):
                    items.extend(self._flatten_dict(v[0], new_key, sep=sep).items())
                else:
                    items.append((new_key, str(v)))
            else:
                items.append((new_key, v))
        return dict(items)

    def convert_file(self, input_file: Path, output_dir: Path) -> str:
        """Convert a single JSON file to CSV"""
        try:
            # Read JSON file
            with open(input_file, 'r', encoding='utf-8') as f:
                data = json.load(f)

            # Handle World Bank API response format
            df = self._handle_world_bank_response(data)
            
            # Flatten nested structures if any exist
            if any(isinstance(v, (dict, list)) for v in df.iloc[0]):
                flattened_data = [self._flatten_dict(row) for _, row in df.iterrows()]
                df = pd.DataFrame(flattened_data)

            # Fill empty values with 'nil'
            df.fillna('nil', inplace=True)

            # Create output filename
            output_file = output_dir / f"{input_file.stem}.csv"
            
            # Save as CSV
            df.to_csv(output_file, index=False, encoding='utf-8')
            
            self.logger.info(f"Successfully converted {input_file.name} to {output_file.name}")
            self.logger.info(f"Number of rows in {output_file.name}: {len(df)}")
            return str(output_file)

        except Exception as e:
            self.logger.error(f"Error converting {input_file.name}: {str(e)}")
            raise

    def convert_directory(self, input_dir: str = "output", output_dir: str = "csv_output") -> Dict[str, str]:
        """Convert all JSON files in a directory to CSV"""
        input_path = Path(input_dir)
        output_path = Path(output_dir)
        
        if not input_path.exists():
            raise FileNotFoundError(f"Input directory '{input_dir}' does not exist")
        
        # Create output directory if it doesn't exist
        output_path.mkdir(exist_ok=True)
        
        # Dictionary to store conversion results
        results = {}
        
        # Process each JSON file
        json_files = list(input_path.glob("*.json"))
        if not json_files:
            self.logger.warning(f"No JSON files found in {input_dir}")
            return results

        for json_file in json_files:
            try:
                output_file = self.convert_file(json_file, output_path)
                results[json_file.stem] = output_file
                self.logger.info(f"Successfully processed {json_file.name}")
            except Exception as e:
                self.logger.error(f"Failed to convert {json_file.name}: {str(e)}")
                continue
        
        return results

def main():
    try:
        converter = JsonToCsvConverter()
        
        # Convert all JSON files
        print("Starting JSON to CSV conversion...")
        results = converter.convert_directory()
        
        if results:
            # Print results
            print("\nConversion Results:")
            print("-" * 50)
            for source, output in results.items():
                print(f"Converted {source}.json -> {Path(output).name}")
            print("-" * 50)
            print(f"Total files converted: {len(results)}")
        else:
            print("No files were converted.")
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main() 