"""
知识库向量存储模块
支持关键词检索和向量检索
"""
import os
import json
import pickle
from typing import List, Dict, Any, Optional, Tuple
from collections import defaultdict
import re

# 尝试导入向量化库
try:
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics.pairwise import cosine_similarity
    HAS_SKLEARN = True
except ImportError:
    HAS_SKLEARN = False
    print("[WARN] sklearn未安装，将使用关键词检索")


class KnowledgeChunk:
    """知识块"""

    def __init__(self, content: str, source: str, chunk_type: str, metadata: Dict = None):
        self.content = content
        self.source = source
        self.chunk_type = chunk_type  # 'stock_report', 'industry_report', 'db_data'
        self.metadata = metadata or {}
        # 生成短id
        self.id = self._generate_id()

    def _generate_id(self) -> str:
        """生成简短ID"""
        import hashlib
        return hashlib.md5(self.content[:100].encode()).hexdigest()[:8]

    def to_dict(self) -> Dict:
        return {
            "id": self.id,
            "content": self.content,
            "source": self.source,
            "chunk_type": self.chunk_type,
            "metadata": self.metadata
        }


class VectorStore:
    """向量知识库"""

    def __init__(self, persist_path: Optional[str] = None):
        self.persist_path = persist_path
        self.chunks: List[KnowledgeChunk] = []
        self.vectorizer = None
        self.tfidf_matrix = None
        self._indexed = False

    def add_chunk(self, chunk: KnowledgeChunk):
        """添加知识块"""
        self.chunks.append(chunk)
        self._indexed = False

    def add_chunks(self, chunks: List[KnowledgeChunk]):
        """批量添加知识块"""
        self.chunks.extend(chunks)
        self._indexed = False

    def build_index(self):
        """构建TF-IDF索引"""
        if not HAS_SKLEARN:
            print("[WARN] sklearn未安装，跳过索引构建")
            return

        if not self.chunks:
            print("[WARN] 知识库为空")
            return

        print(f"[INFO] 正在构建TF-IDF索引 ({len(self.chunks)} 个知识块)...")
        texts = [chunk.content for chunk in self.chunks]

        self.vectorizer = TfidfVectorizer(
            max_features=5000,
            stop_words='english' if not self._is_chinese(texts[0]) else None
        )
        self.tfidf_matrix = self.vectorizer.fit_transform(texts)
        self._indexed = True
        print("[OK] 索引构建完成")

    def _is_chinese(self, text: str) -> bool:
        """判断是否为中文"""
        return bool(re.search(r'[\u4e00-\u9fff]', text))

    def search(self, query: str, top_k: int = 5, chunk_type: Optional[str] = None) -> List[Tuple[KnowledgeChunk, float]]:
        """
        搜索相关知识块

        Args:
            query: 查询文本
            top_k: 返回数量
            chunk_type: 过滤类型

        Returns:
            [(chunk, score), ...]
        """
        if not self.chunks:
            return []

        # 过滤类型
        candidates = self.chunks
        if chunk_type:
            candidates = [c for c in candidates if c.chunk_type == chunk_type]

        if not candidates:
            return []

        # 使用TF-IDF相似度
        if HAS_SKLEARN and self._indexed and self.vectorizer:
            try:
                query_vec = self.vectorizer.transform([query])
                # 只在候选chunk中搜索
                candidate_indices = [self.chunks.index(c) for c in candidates]
                relevant_vectors = self.tfidf_matrix[candidate_indices]
                similarities = cosine_similarity(query_vec, relevant_vectors)[0]

                results = []
                for i, idx in enumerate(candidate_indices):
                    results.append((self.chunks[idx], float(similarities[i])))

                results.sort(key=lambda x: x[1], reverse=True)
                return results[:top_k]
            except Exception as e:
                print(f"[WARN] 向量搜索失败: {e}, 使用关键词搜索")

        # 降级：关键词搜索
        return self._keyword_search(query, candidates, top_k)

    def _keyword_search(self, query: str, chunks: List[KnowledgeChunk], top_k: int) -> List[Tuple[KnowledgeChunk, float]]:
        """关键词搜索"""
        keywords = self._extract_keywords(query)
        results = []

        for chunk in chunks:
            score = 0
            content_lower = chunk.content.lower()
            query_lower = query.lower()

            # 精确包含给高分
            if query_lower in content_lower:
                score += 10

            # 关键词匹配
            for kw in keywords:
                if kw.lower() in content_lower:
                    score += 1

            if score > 0:
                results.append((chunk, score))

        results.sort(key=lambda x: x[1], reverse=True)
        return [(r[0], r[1] / 20) for r in results[:top_k]]  # 归一化分数

    def _extract_keywords(self, text: str) -> List[str]:
        """提取关键词"""
        # 简单实现：提取连续的中文或英文词组
        chinese_words = re.findall(r'[\u4e00-\u9fff]{2,}', text)
        english_words = re.findall(r'[a-zA-Z]{3,}', text)
        return chinese_words + english_words

    def save(self, path: Optional[str] = None):
        """保存知识库"""
        save_path = path or self.persist_path
        if not save_path:
            print("[WARN] 未指定保存路径")
            return

        # 确保目录存在
        os.makedirs(os.path.dirname(save_path), exist_ok=True)

        data = {
            "chunks": [c.to_dict() for c in self.chunks],
            "has_index": self._indexed
        }

        with open(save_path, 'wb') as f:
            pickle.dump(data, f)

        print(f"[OK] 知识库已保存: {save_path}")

    def load(self, path: Optional[str] = None):
        """加载知识库"""
        load_path = path or self.persist_path
        if not load_path or not os.path.exists(load_path):
            print(f"[INFO] 知识库文件不存在: {load_path}")
            return

        try:
            with open(load_path, 'rb') as f:
                data = pickle.load(f)

            self.chunks = []
            for c in data["chunks"]:
                chunk = KnowledgeChunk(
                    content=c["content"],
                    source=c["source"],
                    chunk_type=c["chunk_type"],
                    metadata=c.get("metadata", {})
                )
                self.chunks.append(chunk)

            if data.get("has_index") and HAS_SKLEARN:
                self.build_index()

            print(f"[OK] 知识库已加载: {len(self.chunks)} 个知识块")
        except Exception as e:
            print(f"[ERROR] 加载知识库失败: {e}")


class KnowledgeBaseBuilder:
    """知识库构建器"""

    def __init__(self, vector_store: VectorStore):
        self.vector_store = vector_store

    def add_reports_to_knowledge_base(self, reports: List[Dict[str, Any]]):
        """将研报添加到知识库"""
        for report in reports:
            text = report.get("text", "")
            if not text:
                continue

            # 分块处理（按段落）
            chunks = self._chunk_text(text, report)

            for chunk in chunks:
                self.vector_store.add_chunk(chunk)

            print(f"[OK] 添加研报知识块: {report['filename']} ({len(chunks)}块)")

    def add_structured_data_to_knowledge_base(self, db_data: List[Dict], table_name: str):
        """将结构化数据添加到知识库"""
        for row in db_data:
            # 将每行数据转换为文本描述
            content = self._row_to_text(row, table_name)
            chunk = KnowledgeChunk(
                content=content,
                source=f"database:{table_name}",
                chunk_type="db_data",
                metadata={"table": table_name, "row": row}
            )
            self.vector_store.add_chunk(chunk)

    def _chunk_text(self, text: str, report: Dict, chunk_size: int = 500) -> List[KnowledgeChunk]:
        """将文本分块"""
        chunks = []

        # 按段落分割
        paragraphs = text.split('\n')
        current_chunk = ""
        current_size = 0

        for para in paragraphs:
            para = para.strip()
            if not para:
                continue

            para_len = len(para)

            if current_size + para_len <= chunk_size:
                current_chunk += para + "\n"
                current_size += para_len
            else:
                # 保存当前块
                if current_chunk:
                    chunks.append(KnowledgeChunk(
                        content=current_chunk.strip(),
                        source=report["filename"],
                        chunk_type=f"{report['type']}_report",
                        metadata={"path": report["path"]}
                    ))
                # 开始新块
                current_chunk = para + "\n"
                current_size = para_len

        # 处理最后一块
        if current_chunk:
            chunks.append(KnowledgeChunk(
                content=current_chunk.strip(),
                source=report["filename"],
                chunk_type=f"{report['type']}_report",
                metadata={"path": report["path"]}
            ))

        return chunks

    def _row_to_text(self, row: Dict, table_name: str) -> str:
        """将数据库行转换为文本"""
        parts = []
        for key, value in row.items():
            if value is not None:
                parts.append(f"{key}: {value}")
        return f"表{table_name}: " + ", ".join(parts)


# 测试
if __name__ == "__main__":


    from pdfExtractor import PDFTextExtractor

    # 提取研报
    extractor = PDFTextExtractor()
    reports = extractor.load_all_reports()

    # 构建知识库
    store = VectorStore()
    builder = KnowledgeBaseBuilder(store)
    builder.add_reports_to_knowledge_base(reports)

    # 构建索引
    store.build_index()

    # 测试搜索
    print("\n" + "=" * 60)
    print("测试搜索: '华润三九 医保'")
    results = store.search("华润三九 医保", top_k=3)
    for chunk, score in results:
        print(f"\n[分数: {score:.3f}] {chunk.source}")
        print(f"内容: {chunk.content[:200]}...")

    # 保存
    store.save(r"C:\Users\34084\Desktop\teddy_cup\task3\knowledge_base\vector_store.pkl")