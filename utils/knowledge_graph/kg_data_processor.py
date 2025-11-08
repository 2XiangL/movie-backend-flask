import pandas as pd
import os
import ast
from pathlib import Path
import sys
from typing import Optional, Dict, Any


class KGDataProcessor:
    """知识图谱数据处理器"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.data_directory = config.get("data_directory", "data")
        self.movies_file = os.path.join(self.data_directory, "tmdb_5000_movies.csv")
        self.credits_file = os.path.join(self.data_directory, "tmdb_5000_credits.csv")
        self.processed_data_file = os.path.join(self.data_directory, "kg_processed_data.pkl")
        self.kg_model_file = os.path.join(self.data_directory, "kg_model.pkl")

    def load_and_process_data(self) -> Optional[pd.DataFrame]:
        """加载并处理数据用于知识图谱构建"""
        try:
            # 检查是否已有处理好的数据
            if os.path.exists(self.processed_data_file):
                print("Loading pre-processed knowledge graph data...")
                return pd.read_pickle(self.processed_data_file)

            # 检查原始数据文件
            if not os.path.exists(self.movies_file):
                raise FileNotFoundError(f"Movies data file not found: {self.movies_file}")
            if not os.path.exists(self.credits_file):
                raise FileNotFoundError(f"Credits data file not found: {self.credits_file}")

            print("Loading movies data...")
            movies_df = pd.read_csv(self.movies_file)
            print(f"Loaded {len(movies_df)} movies")

            print("Loading credits data...")
            credits_df = pd.read_csv(self.credits_file)
            print(f"Loaded {len(credits_df)} credits")

            print("Merging data...")
            # 合并数据
            merged_df = movies_df.merge(credits_df, left_on='id', right_on='movie_id', how='inner')

            print("Processing knowledge graph features...")
            processed_df = self._process_kg_features(merged_df)

            # 保存处理好的数据
            print("Saving processed data...")
            processed_df.to_pickle(self.processed_data_file)

            print(f"Processed {len(processed_df)} records for knowledge graph")
            return processed_df

        except Exception as e:
            print(f"Error processing knowledge graph data: {e}")
            import traceback
            traceback.print_exc()
            return None

    def _process_kg_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """处理知识图谱特征"""
        # 解析JSON格式的特征
        df['genres_parsed'] = df['genres'].apply(self._parse_json_list)
        df['keywords_parsed'] = df['keywords'].apply(self._parse_json_list)
        df['cast_parsed'] = df['cast'].apply(self._parse_json_list)
        df['crew_parsed'] = df['crew'].apply(self._parse_json_list)
        df['production_companies_parsed'] = df['production_companies'].apply(self._parse_json_list)

        # 提取导演
        df['directors'] = df['crew_parsed'].apply(self._extract_directors)

        # 提取年份
        df['year'] = df['release_date'].apply(self._extract_year)

        # 过滤掉必要字段为空的记录
        # 合并后的数据框有title_x和title_y列
        title_col = 'title_x' if 'title_x' in df.columns else 'title'
        id_col = 'id' if 'id' in df.columns else 'movie_id' if 'movie_id' in df.columns else 'id'

        # 检查列是否存在
        if title_col not in df.columns:
            print(f"Warning: No title column found. Available columns: {list(df.columns)}")
            return None
        if id_col not in df.columns:
            print(f"Warning: No ID column found. Available columns: {list(df.columns)}")
            return None

        df = df.dropna(subset=[title_col, id_col])
        df = df[df[title_col].str.strip() != '']

        return df

    def _parse_json_list(self, json_str) -> list:
        """解析JSON格式的字符串列表"""
        if pd.isna(json_str) or json_str == '' or json_str == '[]':
            return []
        try:
            data = ast.literal_eval(json_str)
            if isinstance(data, list):
                return [item['name'] for item in data if isinstance(item, dict) and 'name' in item]
            return []
        except Exception as e:
            return []

    def _extract_directors(self, crew_list) -> list:
        """从工作人员列表中提取导演"""
        if not crew_list:
            return []
        directors = []
        for person in crew_list:
            if isinstance(person, dict) and person.get('job') == 'Director':
                name = person.get('name', '')
                if name:
                    directors.append(name)
        return directors

    def _extract_year(self, date_str) -> str:
        """从日期字符串中提取年份"""
        if pd.isna(date_str) or date_str == '':
            return 'Unknown'
        try:
            return str(date_str).split('-')[0]
        except:
            return 'Unknown'

    def get_data_summary(self, df: pd.DataFrame) -> Dict[str, Any]:
        """获取数据摘要"""
        if df is None or df.empty:
            return {}

        summary = {
            'total_movies': len(df),
            'total_genres': len(set([genre for sublist in df['genres_parsed'] for genre in sublist])),
            'total_directors': len(set([director for sublist in df['directors'] for director in sublist])),
            'total_actors': len(set([actor for sublist in df['cast_parsed'] for actor in sublist])),
            'total_keywords': len(set([keyword for sublist in df['keywords_parsed'] for keyword in sublist])),
            'year_range': f"{df['year'].min()}-{df['year'].max()}" if 'year' in df.columns else 'Unknown',
            'avg_rating': df['vote_average'].mean() if 'vote_average' in df.columns else 0,
            'avg_popularity': df['popularity'].mean() if 'popularity' in df.columns else 0
        }

        return summary

