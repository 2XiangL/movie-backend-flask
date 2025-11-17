from flask import Blueprint, request, jsonify
import sys
import os
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from utils.knowledge_graph.kg_recommender import KnowledgeGraphRecommender
from utils.common import convert_numpy_types
from config.settings import Config

kg_bp = Blueprint('knowledge_graph', __name__)

# 全局推荐器实例
kg_recommender = None

def get_kg_recommender():
    """获取知识图谱推荐器实例（延迟加载）"""
    global kg_recommender
    if kg_recommender is None:
        try:
            # 使用与content-based相同的数据目录配置
            config = {
                "data_directory": Config.DATA_DIRECTORY
            }
            kg_recommender = KnowledgeGraphRecommender(config)
            if not kg_recommender.initialize():
                raise Exception("Failed to initialize knowledge graph recommender")
        except Exception as e:
            raise Exception(f"Failed to initialize knowledge graph recommender: {e}")
    return kg_recommender

@kg_bp.route('/recommend-keyword', methods=['GET'])
def recommend_by_keyword():
    """
    基于关键词的电影推荐（知识图谱）

    Query Parameters:
    - keyword: 关键词 (必需) - 可以是导演、演员、类型、关键词等
    - n: 推荐数量 (可选，默认为10)
    """
    try:
        keyword = request.args.get('keyword')
        if not keyword:
            return jsonify({"error": "Missing required parameter: keyword"}), 400

        n = request.args.get('n', type=int, default=10)
        if n <= 0 or n > 50:
            return jsonify({"error": "Parameter n must be between 1 and 50"}), 400

        rec = get_kg_recommender()
        movie_ids = rec.recommend_by_keyword(keyword, n)

        if not movie_ids:
            return jsonify({
                "keyword": keyword,
                "error": "No movies found matching the keyword",
                "suggestions": "Try different keywords like: director name, actor name, genre, or movie theme"
            }), 404

        recommendations = rec.get_recommendation_details(movie_ids)
        recommendations = convert_numpy_types(recommendations)

        return jsonify({
            "keyword": keyword,
            "recommendations": recommendations,
            "count": len(recommendations),
            "method": "knowledge_graph"
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@kg_bp.route('/recommend-similar', methods=['GET'])
def recommend_similar_movies():
    """
    基于电影的相似推荐（知识图谱）

    Query Parameters:
    - movie: 电影标题 (必需)
    - n: 推荐数量 (可选，默认为10)
    """
    try:
        movie_title = request.args.get('movie')
        if not movie_title:
            return jsonify({"error": "Missing required parameter: movie"}), 400

        n = request.args.get('n', type=int, default=10)
        if n <= 0 or n > 50:
            return jsonify({"error": "Parameter n must be between 1 and 50"}), 400

        rec = get_kg_recommender()

        # 首先检查电影是否存在
        movie_details = rec.get_movie_details(movie_title)
        if not movie_details:
            # 尝试搜索相似的标题
            search_results = rec.search_movies(movie_title, limit=5)
            if search_results:
                return jsonify({
                    "movie": movie_title,
                    "error": "Movie not found exactly",
                    "suggestions": search_results
                }), 404
            else:
                return jsonify({
                    "movie": movie_title,
                    "error": "Movie not found"
                }), 404

        movie_ids = rec.recommend_similar_movies(movie_title, n)

        if not movie_ids:
            return jsonify({
                "movie": movie_title,
                "error": "No similar movies found",
                "original_movie": movie_details
            }), 404

        recommendations = rec.get_recommendation_details(movie_ids)
        recommendations = convert_numpy_types(recommendations)

        return jsonify({
            "movie": movie_title,
            "original_movie": movie_details,
            "similar_movies": recommendations,
            "count": len(recommendations),
            "method": "knowledge_graph_similarity"
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@kg_bp.route('/search', methods=['GET'])
def search_movies():
    """
    搜索电影（知识图谱）

    Query Parameters:
    - q: 搜索关键词 (必需)
    - n: 结果数量 (可选，默认为10)
    """
    try:
        query = request.args.get('q')
        if not query:
            return jsonify({"error": "Missing required parameter: q"}), 400

        n = request.args.get('n', type=int, default=10)
        if n <= 0 or n > 50:
            return jsonify({"error": "Parameter n must be between 1 and 50"}), 400

        rec = get_kg_recommender()
        results = rec.search_movies(query, n)

        return jsonify({
            "query": query,
            "results": results,
            "count": len(results),
            "method": "knowledge_graph_search"
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@kg_bp.route('/details', methods=['GET'])
def movie_details():
    """
    获取电影详细信息（知识图谱）

    Query Parameters:
    - movie: 电影标题 (必需)
    """
    try:
        movie_title = request.args.get('movie')
        if not movie_title:
            return jsonify({"error": "Missing required parameter: movie"}), 400

        rec = get_kg_recommender()
        details = rec.get_movie_details(movie_title)

        if not details:
            return jsonify({"error": f"Movie not found: {movie_title}"}), 404

        # Convert numpy types to Python native types
        details = convert_numpy_types(details)

        return jsonify({
            "movie": movie_title,
            "details": details,
            "method": "knowledge_graph"
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@kg_bp.route('/random', methods=['GET'])
def random_movies():
    """
    获取随机电影推荐（知识图谱）

    Query Parameters:
    - n: 推荐数量 (可选，默认为10)
    """
    try:
        n = request.args.get('n', type=int, default=10)
        if n <= 0 or n > 50:
            return jsonify({"error": "Parameter n must be between 1 and 50"}), 400

        rec = get_kg_recommender()
        random_movies = rec.get_random_movies(n)

        return jsonify({
            "random_movies": random_movies,
            "count": len(random_movies),
            "method": "knowledge_graph_random"
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@kg_bp.route('/info', methods=['GET'])
def system_info():
    """
    获取知识图谱系统信息
    """
    try:
        rec = get_kg_recommender()
        info = rec.get_system_info()

        return jsonify({
            "knowledge_graph_system": info,
            "method": "knowledge_graph"
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@kg_bp.route('/graph-stats', methods=['GET'])
def graph_statistics():
    """
    获取知识图谱统计信息
    """
    try:
        rec = get_kg_recommender()
        stats = rec.knowledge_graph.get_graph_info()

        return jsonify({
            "graph_statistics": stats,
            "method": "knowledge_graph"
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@kg_bp.route('/multi-recommend', methods=['POST'])
def multi_recommend():
    """
    综合推荐接口（知识图谱）

    Request Body:
    - keywords: 关键词列表
    - movie: 参考电影（可选）
    - n: 推荐数量（可选，默认为10）
    """
    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "Missing request body"}), 400

        keywords = data.get('keywords', [])
        movie = data.get('movie')
        n = data.get('n', 10)

        if not keywords and not movie:
            return jsonify({
                "error": "Must provide either keywords or movie for recommendation"
            }), 400

        if n <= 0 or n > 50:
            return jsonify({"error": "Parameter n must be between 1 and 50"}), 400

        rec = get_kg_recommender()
        recommendations = []

        # 基于关键词推荐
        if keywords:
            keyword_results = []
            for keyword in keywords:
                movie_ids = rec.recommend_by_keyword(keyword, n)
                if movie_ids:
                    keyword_results.extend(movie_ids)

            if keyword_results:
                # 去重并限制数量
                unique_movie_ids = list(set(keyword_results))[:n]
                recommendations = rec.get_recommendation_details(unique_movie_ids)

        # 基于电影推荐
        elif movie:
            movie_ids = rec.recommend_similar_movies(movie, n)
            if movie_ids:
                recommendations = rec.get_recommendation_details(movie_ids)

        recommendations = convert_numpy_types(recommendations)

        return jsonify({
            "request": {
                "keywords": keywords,
                "movie": movie,
                "n": n
            },
            "recommendations": recommendations,
            "count": len(recommendations),
            "method": "knowledge_graph_multi"
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

