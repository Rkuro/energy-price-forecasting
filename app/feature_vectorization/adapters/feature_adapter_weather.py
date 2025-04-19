from typing import List, Any, Tuple
import re
import logging
from .feature_adapter import FeatureAdapter
from .horizons import Horizon

log = logging.getLogger(__name__)

class WeatherFeatureAdapter(FeatureAdapter):

    def __init__(self):
        self.feature_vector_size = 168 # 12 features * 14 days

    def can_handle(self, msg_type: str) -> bool:
        return msg_type == "weather"

    def vectorize(self, data: Any, past_data: Any) ->  Tuple[List[Horizon], List[float]]:
        """
        Converts a weather.gov-style forecast JSON into a fixed-length vector.

        Each period produces 12 features:
        - temperature (F)
        - dew point estimate (F, crude)
        - precipitation probability (0-100)
        - average wind speed (mph)
        - max gust wind speed (mph)
        - wind direction (deg from N)
        - isDaytime (0/1)
        - temperature trend (-1/0/1)
        - rain, snow, cloud, storm indicators (0/1 each)

        Total vector = [features for N periods], padded with 0.0 to match self.feature_vector_size.
        """
        # Throw exception quickly if the data is not formatted as expected (ValueError)
        periods = data.get('data').get('forecast').get('periods')
        vector = []

        for period in periods:
            # 1. Temperature
            temperature = self._safe_float(period.get("temperature"))

            # 2. Crude dew point estimate (temp - 4 if high RH, else temp - 10)
            pop = self._safe_float(period.get("probabilityOfPrecipitation", {}).get("value"), 0)
            dew_point = temperature - 4 if pop >= 80 else temperature - 10

            # 3. Precipitation Probability
            precip_prob = pop

            # 4. Wind Speed (average)
            wind_speed_avg = self._parse_avg_wind_speed(period.get("windSpeed", "0 mph"))

            # 5. Wind Speed (gust max)
            wind_speed_max = self._parse_max_wind_speed(period.get("windSpeed", "0 mph"))

            # 6. Wind Direction (° from N)
            wind_dir = self._wind_direction_to_deg(period.get("windDirection", "N"))

            # 7. isDaytime
            is_daytime = 1 if period.get("isDaytime", False) else 0

            # 8. Temp trend: rising=1, falling=-1, else 0
            trend = {"rising": 1, "falling": -1}.get(period.get("temperatureTrend", "").lower(), 0)

            # 9–12. Condition Indicators
            forecast = period.get("shortForecast", "").lower()
            is_rain = 1 if "rain" in forecast else 0
            is_snow = 1 if "snow" in forecast else 0
            is_cloud = 1 if any(k in forecast for k in ["cloud", "overcast"]) else 0
            is_storm = 1 if any(k in forecast for k in ["thunder", "storm", "lightning"]) else 0

            vector.extend([
                temperature,
                dew_point,
                precip_prob,
                wind_speed_avg,
                wind_speed_max,
                wind_dir,
                is_daytime,
                trend,
                is_rain,
                is_snow,
                is_cloud,
                is_storm,
            ])

            if len(vector) >= self.feature_vector_size:
                break

        # Pad if shorter than expected
        if len(vector) < self.feature_vector_size:
            log.info(f"Padding feature vector from {len(vector)} to {self.feature_vector_size}")
            vector.extend([0.0] * (self.feature_vector_size - len(vector)))

        return [Horizon.five_minute, Horizon.one_hour, Horizon.one_day], vector[:self.feature_vector_size]

    def _parse_avg_wind_speed(self, wind_str: str) -> float:
        speeds = re.findall(r'\d+', wind_str)
        if not speeds:
            return 0.0
        values = list(map(int, speeds))
        return sum(values) / len(values)

    def _parse_max_wind_speed(self, wind_str: str) -> float:
        speeds = re.findall(r'\d+', wind_str)
        if not speeds:
            return 0.0
        return max(map(int, speeds))

    def _wind_direction_to_deg(self, direction: str) -> float:
        dir_map = {
            'N': 0, 'NNE': 22.5, 'NE': 45, 'ENE': 67.5,
            'E': 90, 'ESE': 112.5, 'SE': 135, 'SSE': 157.5,
            'S': 180, 'SSW': 202.5, 'SW': 225, 'WSW': 247.5,
            'W': 270, 'WNW': 292.5, 'NW': 315, 'NNW': 337.5
        }
        return dir_map.get(direction.upper(), 0.0)

    def _safe_float(self, value: Any, default: float = 0.0) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    def archive(self, msg: Any) -> Any:
        pass

    def unarchive(self, data: Any) -> Any:
        pass