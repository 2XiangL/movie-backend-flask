"""
IMDb数据管理器 - 为所有推荐器提供IMDb和海报链接
"""

import pandas as pd
import os
import logging
from typing import Dict, Optional

logger = logging.getLogger(__name__)

class IMDbDataManager:
    """IMDb数据管理器，单例模式"""

    _instance = None
    _imdb_data = None
    _initialized = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(IMDbDataManager, cls).__new__(cls)
        return cls._instance

    def initialize(self, data_directory: str, imdb_file_name: str = "imdblink_img_data.csv"):
        """初始化IMDb数据管理器"""
        if self._initialized:
            return True

        try:
            imdb_file_path = os.path.join(data_directory, imdb_file_name)
            self._imdb_data = pd.read_csv(imdb_file_path)
            logger.info(f"IMDb data loaded successfully from {imdb_file_path}")
            logger.info(f"IMDb data shape: {self._imdb_data.shape}")
            self._initialized = True
            return True
        except Exception as e:
            logger.warning(f"Could not load IMDb data: {e}")
            self._imdb_data = pd.DataFrame(columns=['title', 'imdb_url', 'img_url'])
            self._initialized = True
            return False

    def get_imdb_info(self, movie_title: str) -> Optional[Dict]:
        """
        获取电影的IMDb和海报信息

        Args:
            movie_title: 电影标题

        Returns:
            包含imdb_url和poster_url的字典，或None
        """
        if not self._initialized or self._imdb_data is None or self._imdb_data.empty:
            return None

        try:
            # 尝试精确匹配
            imdb_row = self._imdb_data[self._imdb_data['title'].str.lower() == movie_title.lower()]

            if imdb_row.empty:
                # 尝试部分匹配
                imdb_row = self._imdb_data[self._imdb_data['title'].str.contains(movie_title, case=False, na=False)]

            if not imdb_row.empty:
                row = imdb_row.iloc[0]
                return {
                    'imdb_url': row.get('imdb_url'),
                    'poster_url': row.get('img_url')
                }
            return None
        except Exception as e:
            logger.error(f"Error getting IMDb info for {movie_title}: {e}")
            return None

    def enhance_movie_data(self, movie_data: Dict) -> Dict:
        """
        增强电影数据，添加IMDb和海报链接

        Args:
            movie_data: 原始电影数据字典

        Returns:
            增强后的电影数据字典
        """
        if not movie_data:
            return movie_data

        enhanced_data = movie_data.copy()
        title = movie_data.get('title')

        if title:
            imdb_info = self.get_imdb_info(title)
            if imdb_info:
                enhanced_data.update(imdb_info)
            else:
                enhanced_data.update({
                    'imdb_url': None,
                    'poster_url': None
                })

        return enhanced_data

    def enhance_movie_list(self, movie_list: list) -> list:
        """
        增强电影列表，为每部电影添加IMDb和海报链接

        Args:
            movie_list: 电影数据字典列表

        Returns:
            增强后的电影数据字典列表
        """
        if not movie_list:
            return movie_list

        enhanced_list = []
        for movie_data in movie_list:
            enhanced_list.append(self.enhance_movie_data(movie_data))

        return enhanced_list

    @property
    def is_loaded(self) -> bool:
        """检查IMDb数据是否已加载"""
        return self._initialized and self._imdb_data is not None and not self._imdb_data.empty

    @property
    def data_shape(self) -> tuple:
        """获取IMDb数据形状"""
        if self._imdb_data is not None:
            return self._imdb_data.shape
        return (0, 0)


# 全局单例实例
imdb_manager = IMDbDataManager()

