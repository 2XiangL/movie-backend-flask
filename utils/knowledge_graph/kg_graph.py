import networkx as nx
import pandas as pd
from collections import defaultdict
from typing import Optional, Dict, List, Any, Tuple
import pickle
import os


class KnowledgeGraph:
    """知识图谱构建和管理"""

    def __init__(self):
        self.graph = nx.Graph()
        self.node_types = defaultdict(list)
        self.initialized = False

    def build_graph_from_data(self, df: pd.DataFrame):
        """从处理好的数据构建知识图谱"""
        if df is None or df.empty:
            raise ValueError("Data is empty or None")

        print("Building knowledge graph from processed data...")

        for _, row in df.iterrows():
            movie_id = f"movie_{row['id']}"
            # 处理合并后的数据框中的标题列
            title_col = 'title_x' if 'title_x' in df.columns else 'title'
            movie_title = str(row.get(title_col, 'Unknown'))

            # 添加电影节点
            self.graph.add_node(movie_id,
                               type='movie',
                               title=movie_title,
                               year=str(row.get('year', 'Unknown')),
                               rating=float(row.get('vote_average', 0)),
                               popularity=float(row.get('popularity', 0)),
                               vote_count=int(row.get('vote_count', 0)))

            # 添加类型节点和关系
            genres = row.get('genres_parsed', [])
            for genre in genres[:5]:  # 限制类型数量
                if genre:
                    genre_id = f"genre_{self._sanitize_name(genre)}"
                    self.graph.add_node(genre_id, type='genre', name=genre)
                    self.graph.add_edge(movie_id, genre_id, relation='has_genre')

            # 添加导演节点和关系
            directors = row.get('directors', [])
            for director in directors[:3]:  # 限制导演数量
                if director:
                    director_id = f"director_{self._sanitize_name(director)}"
                    self.graph.add_node(director_id, type='director', name=director)
                    self.graph.add_edge(movie_id, director_id, relation='directed_by')

            # 添加演员节点和关系
            cast = row.get('cast_parsed', [])
            for actor in cast[:5]:  # 限制演员数量
                if actor:
                    actor_id = f"actor_{self._sanitize_name(actor)}"
                    self.graph.add_node(actor_id, type='actor', name=actor)
                    self.graph.add_edge(movie_id, actor_id, relation='starring')

            # 添加关键词节点和关系
            keywords = row.get('keywords_parsed', [])
            for keyword in keywords[:5]:  # 限制关键词数量
                if keyword:
                    keyword_id = f"keyword_{self._sanitize_name(keyword)}"
                    self.graph.add_node(keyword_id, type='keyword', name=keyword)
                    self.graph.add_edge(movie_id, keyword_id, relation='has_keyword')

            # 添加制作公司节点和关系
            companies = row.get('production_companies_parsed', [])
            for company in companies[:3]:  # 限制制作公司数量
                if company:
                    company_id = f"company_{self._sanitize_name(company)}"
                    self.graph.add_node(company_id, type='company', name=company)
                    self.graph.add_edge(movie_id, company_id, relation='produced_by')

        print(f"Knowledge graph built: {self.graph.number_of_nodes()} nodes, {self.graph.number_of_edges()} edges")

        # 统计节点类型
        self._categorize_nodes()
        self.initialized = True

        return True

    def _sanitize_name(self, name: str) -> str:
        """清理名称用于创建节点ID"""
        return str(name).replace(' ', '_').replace('/', '_').replace('-', '_').replace(':', '_')[:50]

    def _categorize_nodes(self):
        """分类节点"""
        self.node_types.clear()
        for node, attrs in self.graph.nodes(data=True):
            node_type = attrs.get('type', 'unknown')
            self.node_types[node_type].append(node)

        print("Node type statistics:")
        for node_type, nodes in self.node_types.items():
            print(f"  {node_type}: {len(nodes)} nodes")

    def find_movies_by_keyword(self, keyword: str, top_n: int = 10) -> List[str]:
        """根据关键词查找电影"""
        if not self.initialized:
            return []

        keyword = keyword.lower()
        movie_scores = []

        # 查找匹配的节点
        matching_nodes = []
        for node, attrs in self.graph.nodes(data=True):
            if (attrs.get('type') in ['movie', 'genre', 'director', 'actor', 'keyword'] and
                keyword in str(attrs.get('title', '')).lower() or
                keyword in str(attrs.get('name', '')).lower()):
                matching_nodes.append(node)

        # 为每个电影计算相关性分数
        movie_nodes = self.node_types.get('movie', [])
        for movie_node in movie_nodes:
            score = self._calculate_movie_score(movie_node, matching_nodes)
            if score > 0:
                movie_data = self.graph.nodes[movie_node]
                # 综合分数：相关性分数 + 评分 + 流行度
                relevance_score = score
                rating = movie_data.get('rating', 0) / 10  # 归一化到0-1
                popularity = min(movie_data.get('popularity', 0) / 100, 1)  # 归一化到0-1
                vote_count = min(movie_data.get('vote_count', 0) / 1000, 1)  # 归一化到0-1

                final_score = relevance_score * 0.5 + rating * 0.3 + popularity * 0.1 + vote_count * 0.1
                movie_scores.append((movie_node, final_score))

        # 排序并返回前N个
        movie_scores.sort(key=lambda x: x[1], reverse=True)
        return [movie_id for movie_id, _ in movie_scores[:top_n]]

    def find_similar_movies(self, movie_title: str, top_n: int = 10) -> List[str]:
        """根据电影标题查找相似电影"""
        if not self.initialized:
            return []

        movie_node = self.find_movie_by_title(movie_title)
        if not movie_node:
            return []

        # 获取该电影的所有特征
        movie_features = set(self.graph.neighbors(movie_node))
        movie_data = self.graph.nodes[movie_node]

        similar_movies = []
        movie_nodes = self.node_types.get('movie', [])

        for other_movie in movie_nodes:
            if other_movie == movie_node:
                continue

            # 计算特征相似度
            other_features = set(self.graph.neighbors(other_movie))
            common_features = movie_features.intersection(other_features)

            if len(common_features) > 0:
                # Jaccard相似度
                jaccard_similarity = len(common_features) / len(movie_features.union(other_features))

                # 评分相似度
                other_data = self.graph.nodes[other_movie]
                rating_similarity = 1 - abs(movie_data.get('rating', 0) - other_data.get('rating', 0)) / 10

                # 年份相似度
                year_similarity = self._calculate_year_similarity(
                    movie_data.get('year', '0'), other_data.get('year', '0')
                )

                # 综合相似度
                combined_similarity = (jaccard_similarity * 0.6 +
                                     rating_similarity * 0.2 +
                                     year_similarity * 0.2)

                similar_movies.append((other_movie, combined_similarity))

        # 排序并返回前N个
        similar_movies.sort(key=lambda x: x[1], reverse=True)
        return [movie_id for movie_id, _ in similar_movies[:top_n]]

    def _calculate_movie_score(self, movie_node: str, matching_nodes: List[str]) -> float:
        """计算电影与匹配节点的相关性分数"""
        if not matching_nodes:
            return 0

        movie_neighbors = set(self.graph.neighbors(movie_node))
        score = 0

        # 直接匹配
        if movie_node in matching_nodes:
            score += 2.0

        # 特征匹配
        for matching_node in matching_nodes:
            if matching_node in movie_neighbors:
                node_type = self.graph.nodes[matching_node].get('type')
                # 不同类型特征给予不同权重
                if node_type == 'director':
                    score += 1.5
                elif node_type == 'genre':
                    score += 1.0
                elif node_type == 'actor':
                    score += 0.8
                elif node_type == 'keyword':
                    score += 0.6

        return score

    def _calculate_year_similarity(self, year1: str, year2: str) -> float:
        """计算年份相似度"""
        try:
            y1 = int(str(year1)[:4])
            y2 = int(str(year2)[:4])
            year_diff = abs(y1 - y2)
            # 年份差距在5年内认为相似
            return max(0, 1 - year_diff / 10)
        except:
            return 0

    def find_movie_by_title(self, title: str) -> Optional[str]:
        """根据标题查找电影节点"""
        title_lower = title.lower()
        for node, attrs in self.graph.nodes(data=True):
            if (attrs.get('type') == 'movie' and
                title_lower in str(attrs.get('title', '')).lower()):
                return node
        return None

    def get_movie_details(self, movie_id: str) -> Optional[Dict[str, Any]]:
        """获取电影详细信息"""
        if movie_id not in self.graph.nodes:
            return None

        movie_data = dict(self.graph.nodes[movie_id])

        # 获取相关特征
        neighbors = list(self.graph.neighbors(movie_id))
        movie_data['genres'] = []
        movie_data['directors'] = []
        movie_data['actors'] = []
        movie_data['keywords'] = []
        movie_data['companies'] = []

        for neighbor in neighbors:
            neighbor_attrs = self.graph.nodes[neighbor]
            neighbor_type = neighbor_attrs.get('type')
            neighbor_name = neighbor_attrs.get('name', '')

            if neighbor_type == 'genre' and neighbor_name:
                movie_data['genres'].append(neighbor_name)
            elif neighbor_type == 'director' and neighbor_name:
                movie_data['directors'].append(neighbor_name)
            elif neighbor_type == 'actor' and neighbor_name:
                movie_data['actors'].append(neighbor_name)
            elif neighbor_type == 'keyword' and neighbor_name:
                movie_data['keywords'].append(neighbor_name)
            elif neighbor_type == 'company' and neighbor_name:
                movie_data['companies'].append(neighbor_name)

        return movie_data

    def get_graph_info(self) -> Dict[str, Any]:
        """获取知识图谱统计信息"""
        if not self.initialized:
            return {}

        return {
            'total_nodes': self.graph.number_of_nodes(),
            'total_edges': self.graph.number_of_edges(),
            'node_types': {node_type: len(nodes) for node_type, nodes in self.node_types.items()},
            'movie_count': len(self.node_types.get('movie', [])),
            'genre_count': len(self.node_types.get('genre', [])),
            'director_count': len(self.node_types.get('director', [])),
            'actor_count': len(self.node_types.get('actor', [])),
            'keyword_count': len(self.node_types.get('keyword', [])),
            'company_count': len(self.node_types.get('company', []))
        }

    def save_graph(self, filepath: str):
        """保存知识图谱"""
        try:
            graph_data = {
                'graph': self.graph,
                'node_types': dict(self.node_types),
                'initialized': self.initialized
            }
            with open(filepath, 'wb') as f:
                pickle.dump(graph_data, f)
            print(f"Knowledge graph saved to {filepath}")
            return True
        except Exception as e:
            print(f"Error saving knowledge graph: {e}")
            return False

    def load_graph(self, filepath: str) -> bool:
        """加载知识图谱"""
        try:
            with open(filepath, 'rb') as f:
                graph_data = pickle.load(f)

            self.graph = graph_data['graph']
            self.node_types = defaultdict(list, graph_data['node_types'])
            self.initialized = graph_data['initialized']

            print(f"Knowledge graph loaded from {filepath}")
            print(f"Loaded {self.graph.number_of_nodes()} nodes and {self.graph.number_of_edges()} edges")
            return True
        except Exception as e:
            print(f"Error loading knowledge graph: {e}")
            return False

    def search_movies(self, query: str, limit: int = 10) -> List[Dict[str, Any]]:
        """搜索电影"""
        if not self.initialized:
            return []

        query = query.lower()
        results = []

        for node, attrs in self.graph.nodes(data=True):
            if attrs.get('type') == 'movie':
                title = str(attrs.get('title', ''))
                if query in title.lower():
                    movie_data = self.get_movie_details(node)
                    if movie_data:
                        results.append(movie_data)
                        if len(results) >= limit:
                            break

        return results

