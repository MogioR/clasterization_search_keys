import numpy as np

from tqdm import tqdm
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import CountVectorizer


class ClusteringService:
    @staticmethod
    def cluster_list(data: list, duplicates_uniqueness: float):
        vectors, duplicate_matrix = ClusteringService.get_duplicate_matrix(data)
        return ClusteringService.cluster(vectors, duplicate_matrix, duplicates_uniqueness)

    @staticmethod
    def get_duplicate_matrix(data: list):
        # Vectorize
        vectorizer = CountVectorizer(token_pattern=r'[^ ]*')
        transformed = vectorizer.fit_transform(data)
        vectors = transformed.toarray()

        # Del empty
        tokens = vectorizer.get_feature_names_out()
        empty = -1
        for i, token in enumerate(tokens):
            if token == '':
                empty = i
                break
        if empty != -1:
            for vec in vectors:
                vec[empty] = 0

        # Get duplicate_matrix

        duplicate_matrix = cosine_similarity(vectors)

        return vectors, duplicate_matrix

    @staticmethod
    def cluster(vectors: np.ndarray, duplicate_matrix: np.ndarray,  duplicates_uniqueness):
        duplicate_classes = [-1 for _ in range(len(vectors))]

        for i in tqdm(range(len(duplicate_matrix)), total=len(duplicate_matrix)):
            if duplicate_classes[i] == -1:
                max_id = i
                duplicates = []
                for j in range(len(duplicate_matrix[i])):
                    if round(duplicate_matrix[i][j], 2) >= duplicates_uniqueness and duplicate_classes[j] == -1:
                        duplicate_flag = True

                        for duplicate in duplicates:
                            if round(duplicate_matrix[duplicate][j], 2) < duplicates_uniqueness:
                                duplicate_flag = False
                                break
                        if duplicate_flag:
                            duplicates.append(j)

                for duplicate_id in duplicates:
                    if duplicate_classes[duplicate_id] == -1:
                        duplicate_classes[duplicate_id] = max_id

        return duplicate_classes

