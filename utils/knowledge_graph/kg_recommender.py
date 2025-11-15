from typing import List, Dict, Any, Optional
from .kg_graph import KnowledgeGraph
from .kg_data_processor import KGDataProcessor
import os


class KnowledgeGraphRecommender:
    """知识图谱推荐器"""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.data_processor = KGDataProcessor(config)
        self.knowledge_graph = KnowledgeGraph()
        self.initialized = False

    def initialize(self) -> bool:
        """初始化推荐器"""
        try:
            print("Initializing Knowledge Graph Recommender...")

            # 检查是否已有训练好的模型
            kg_model_file = os.path.join(
                self.config.get("data_directory", "data"),
                "kg_model.pkl"
            )

            if os.path.exists(kg_model_file):
                print("Loading pre-built knowledge graph...")
                if self.knowledge_graph.load_graph(kg_model_file):
                    self.initialized = True
                    print("Knowledge Graph recommender initialized successfully!")
                    return True

            # 加载并处理数据
            print("Loading and processing data for knowledge graph...")
            processed_data = self.data_processor.load_and_process_data()
            if processed_data is None:
                print("Failed to load data")
                return False

            # 构建知识图谱
            print("Building knowledge graph...")
            if not self.knowledge_graph.build_graph_from_data(processed_data):
                print("Failed to build knowledge graph")
                return False

            # 保存知识图谱
            if not self.knowledge_graph.save_graph(kg_model_file):
                print("Warning: Failed to save knowledge graph")

            self.initialized = True
            print("Knowledge Graph recommender initialized successfully!")

            # 显示统计信息
            info = self.knowledge_graph.get_graph_info()
            print(f"Knowledge Graph Statistics:")
            print(f"  Total nodes: {info.get('total_nodes', 0)}")
            print(f"  Total edges: {info.get('total_edges', 0)}")
            print(f"  Movies: {info.get('movie_count', 0)}")
            print(f"  Genres: {info.get('genre_count', 0)}")
            print(f"  Directors: {info.get('director_count', 0)}")
            print(f"  Actors: {info.get('actor_count', 0)}")

            return True

        except Exception as e:
            print(f"Error initializing Knowledge Graph recommender: {e}")
            import traceback
            traceback.print_exc()
            return False

    def recommend_by_keyword(self, keyword: str, top_n: int = 10) -> List[str]:
        """基于关键词推荐电影"""
        if not self.initialized:
            print("Knowledge Graph recommender not initialized")
            return []

        try:
            movie_ids = self.knowledge_graph.find_movies_by_keyword(keyword, top_n)
            return movie_ids
        except Exception as e:
            print(f"Error in keyword recommendation: {e}")
            return []

    def recommend_similar_movies(self, movie_title: str, top_n: int = 10) -> List[str]:
        """基于电影推荐相似电影"""
        if not self.initialized:
            print("Knowledge Graph recommender not initialized")
            return []

        try:
            movie_ids = self.knowledge_graph.find_similar_movies(movie_title, top_n)
            return movie_ids
        except Exception as e:
            print(f"Error in similar movie recommendation: {e}")
            return []

    def get_recommendation_details(self, movie_ids: List[str]) -> List[Dict[str, Any]]:
        """获取推荐电影的详细信息"""
        if not self.initialized:
            return []

        try:
            details = []
            for movie_id in movie_ids:
                movie_data = self.knowledge_graph.get_movie_details(movie_id)
                if movie_data:
                    # 标准化输出格式
                    standardized_data = {
                        'movie_id': int(movie_id.split('_')[1]) if '_' in movie_id else None,
                        'title': movie_data.get('title', 'Unknown'),
                        'year': movie_data.get('year', 'Unknown'),
                        'rating': movie_data.get('rating', 0),
                        'popularity': movie_data.get('popularity', 0),
                        'vote_count': movie_data.get('vote_count', 0),
                        'genres': movie_data.get('genres', []),
                        'directors': movie_data.get('directors', []),
                        'actors': movie_data.get('actors', [])[:5],  # 限制演员数量
                        'keywords': movie_data.get('keywords', [])[:5],  # 限制关键词数量
                        'companies': movie_data.get('companies', [])[:3],  # 限制制作公司数量
                        'type': 'knowledge_graph_recommendation',
                        'score': 1.0  # 知识图谱默认分数
                    }
                    details.append(standardized_data)

            return details
        except Exception as e:
            print(f"Error getting recommendation details: {e}")
            return []

    def search_movies(self, query: str, limit: int = 10) -> List[Dict[str, Any]]:
        """搜索电影"""
        if not self.initialized:
            return []

        try:
            results = self.knowledge_graph.search_movies(query, limit)

            # 标准化输出格式
            standardized_results = []
            for movie_data in results:
                standardized_data = {
                    'movie_id': int(movie_data.get('movie_id', 0)) if movie_data.get('movie_id') else None,
                    'title': movie_data.get('title', 'Unknown'),
                    'year': movie_data.get('year', 'Unknown'),
                    'rating': movie_data.get('rating', 0),
                    'popularity': movie_data.get('popularity', 0),
                    'vote_count': movie_data.get('vote_count', 0),
                    'type': 'knowledge_graph_search'
                }
                standardized_results.append(standardized_data)

            return standardized_results
        except Exception as e:
            print(f"Error searching movies: {e}")
            return []

    def get_movie_details(self, movie_title: str) -> Optional[Dict[str, Any]]:
        """获取指定电影的详细信息"""
        if not self.initialized:
            return None

        try:
            movie_node = self.knowledge_graph.find_movie_by_title(movie_title)
            if not movie_node:
                return None

            movie_data = self.knowledge_graph.get_movie_details(movie_node)
            if not movie_data:
                return None

            # 标准化输出格式
            standardized_data = {
                'movie_id': int(movie_node.split('_')[1]) if '_' in movie_node else None,
                'title': movie_data.get('title', 'Unknown'),
                'year': movie_data.get('year', 'Unknown'),
                'rating': movie_data.get('rating', 0),
                'popularity': movie_data.get('popularity', 0),
                'vote_count': movie_data.get('vote_count', 0),
                'genres': movie_data.get('genres', []),
                'directors': movie_data.get('directors', []),
                'actors': movie_data.get('actors', []),
                'keywords': movie_data.get('keywords', []),
                'companies': movie_data.get('companies', []),
                'type': 'knowledge_graph_details'
            }

            return standardized_data
        except Exception as e:
            print(f"Error getting movie details: {e}")
            return None

    def get_system_info(self) -> Dict[str, Any]:
        """获取系统信息"""
        if not self.initialized:
            return {
                'initialized': False,
                'error': 'Knowledge Graph recommender not initialized'
            }

        try:
            graph_info = self.knowledge_graph.get_graph_info()
            return {
                'method': 'Knowledge Graph-based Recommendation',
                'initialized': True,
                'graph_statistics': graph_info,
                'features': [
                    'Multi-hop entity connections',
                    'Semantic similarity',
                    'Genre, director, actor, keyword relationships',
                    'Company and production relationships',
                    'Temporal similarity (year-based)',
                    'Combined relevance scoring'
                ],
                'capabilities': [
                    'Entity-based search',
                    'Similar movie recommendations',
                    'Multi-modal similarity (content + metadata)',
                    'Cross-domain connections (actor-director, etc.)'
                ]
            }
        except Exception as e:
            return {
                'initialized': False,
                'error': str(e)
            }

    def get_random_movies(self, n: int = 10) -> List[Dict[str, Any]]:
        """获取随机电影推荐"""
        if not self.initialized:
            return []

        try:
            import random
            movie_nodes = self.knowledge_graph.node_types.get('movie', [])
            if not movie_nodes:
                return []

            # 随机选择电影
            selected_nodes = random.sample(
                min(movie_nodes, n),
                min(len(movie_nodes), n)
            )

            # 获取详细信息
            movie_ids = [node for node in selected_nodes]
            return self.get_recommendation_details(movie_ids)
        except Exception as e:
            print(f"Error getting random movies: {e}")
            return []

