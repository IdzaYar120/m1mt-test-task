import pandas as pd
from arcgis.gis import GIS
from arcgis.features import FeatureLayer
from datetime import datetime
import sys


CONFIG = {
    # TODO: Swap these with real credentials before running!
    "ARCGIS_USERNAME": "idzayar",
    "ARCGIS_PASSWORD": "6Eu4ng2LMS4hwX.",
    
    "FEATURE_LAYER_ID": "169f1042c8d845089afe9495e8a8cafb",
    
    "SOURCE_DATA_PATH": "https://docs.google.com/spreadsheets/d/12846JbH2PwR0wN8eLVnosg4xujw-04gKyyD6RuElc-4/export?format=csv"
}

class GISTaskProcessor:
    def __init__(self, config):
        self.config = config
        self.raw_data = None
        self.processed_data = []
        
    def log(self, message):
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}")

    def load_data(self):
        self.log("Fetching data from Google Sheets...")
        try:
            self.raw_data = pd.read_csv(self.config['SOURCE_DATA_PATH'])
            self.log(f"Got {len(self.raw_data)} rows. Looks good.")
        except Exception as e:
            self.log(f"Failed to download data: {e}")
            sys.exit(1)

    def _clean_coordinate(self, val):
        if isinstance(val, str):
            return float(val.replace(',', '.'))
        return float(val)

    def transform_data(self):
        self.log("Starting data transformation...")
        value_cols = [f"Значення {i}" for i in range(1, 11)]
        new_rows = []

        for _, row in self.raw_data.iterrows():
            try:
                current_values = row[value_cols].apply(pd.to_numeric, errors='coerce').fillna(0).astype(int)
            except ValueError:
                continue

            max_val = current_values.max()
            if max_val <= 0:
                continue

            base_row = row.drop(labels=value_cols).to_dict()
            
            try:
                base_row['long'] = self._clean_coordinate(base_row['long'])
                base_row['lat'] = self._clean_coordinate(base_row['lat'])
                
                
            except Exception as e:
                self.log(f"Skipping row due to bad format: {e}")
                continue

            for i in range(max_val):
                new_row = base_row.copy()
                for col in value_cols:
                    val = row[col]
                    try:
                        original_val = int(float(val)) if pd.notnull(val) and val != '' else 0
                    except ValueError:
                        original_val = 0     
                    new_row[col] = 1 if i < original_val else 0
                
                new_rows.append(new_row)

        self.processed_data = pd.DataFrame(new_rows)
        self.log(f"Transformation done. Expanded to {len(self.processed_data)} points.")

    def upload_to_arcgis(self):
        self.log("Connecting to ArcGIS Online...")
        try:
            gis = GIS("https://www.arcgis.com", 
                      self.config['ARCGIS_USERNAME'], 
                      self.config['ARCGIS_PASSWORD'])
            
            item = gis.content.get(self.config['FEATURE_LAYER_ID'])
            if not item:
                raise Exception("Layer ID is wrong or permissions error.")
            
            feature_layer = item.layers[0]
            self.log(f"Found layer: {item.title}")

        except Exception as e:
            self.log(f"Auth failed: {e}")
            sys.exit(1)

      
        column_map = {
            "Дата": "date_1",      
            "Область": "Область",  
            "Місто": "city",      
            "long": "long",
            "lat": "lat"
        }
        for i in range(1, 11):
            column_map[f"Значення {i}"] = f"value_{i}"

        features_to_add = []
        
        for _, row in self.processed_data.iterrows():
            attributes = {}
            for csv_col, gis_col in column_map.items():
                val = row[csv_col]
                if gis_col == "date_1" and isinstance(val, (pd.Timestamp, datetime)):
                     val = val.strftime("%d.%m.%Y")
                
                attributes[gis_col] = val
            
            geometry = {
                "x": row['long'],
                "y": row['lat'],
                "spatialReference": {"wkid": 4326}
            }

            features_to_add.append({
                "attributes": attributes,
                "geometry": geometry
            })

        if features_to_add:
            self.log(f"Pushing {len(features_to_add)} features to cloud...")
            try:
                result = feature_layer.edit_features(adds=features_to_add, rollback_on_failure=True)
                
                successes = len([r for r in result['addResults'] if r['success']])
                self.log(f"Success! Added {successes} features.")
            except Exception as e:
                self.log(f"Upload failed: {e}")
        else:
            self.log("No data to upload.")

    def run(self):
        self.load_data()
        self.transform_data()
        self.upload_to_arcgis()
        self.log("Job finished.")

if __name__ == "__main__":
    processor = GISTaskProcessor(CONFIG)
    processor.run()