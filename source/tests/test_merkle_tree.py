import math
from unittest import TestCase
from unittest.mock import patch

import pickle

from source.src.merkle_tree import MerkleTree


class MerkleTreeTest(TestCase):
    test_data_even = ["7", "8", "9", "10", "11", "12", "13", "14"]
    test_data_even_hashes = [
        ["bf57020a599b6ca72c29faca759d2f5c782b0fd1b611ed529e0ea422c28daf36"],  # Depth 0
        ["5e86592667a4d216699c135c5c814b90ebd6083ba5916886e069b8bebfd873ea",
         "5e296e27d6ebc7a30a9ca43974a3a165c11672aeadb0609e3b98238c5a9cc3a2"],  # Depth 1
        ["ada17dcc2d615cc0d982a538b618f45441e798c38b1df9f3dd273e386eba1363",
         "76d4c0b6f7f7cc7122ea4e442c4d2a4af4578855a1dfd3803db52a38b48be8f9",
         "6bce5a3c8b73421b8575f01a0d2c0edb8e2c60eaca11c0452e10597d19bf32a2",
         "65490299ee72d212e57f8b1e48ad29236608927f12c194f5c081717a3342f746"],  # Depth 2
        ["7902699be42c8a8e46fbbb4501726517e86b22c56a189f7625a6da49081b2451",
         "2c624232cdd221771294dfbb310aca000a0df6ac8b66b696d90ef06fdefb64a3",
         "19581e27de7ced00ff1ce50b2047e7a567c76b1cbaebabe5ef03f7c3017bb5b7",
         "4a44dc15364204a80fe80e9039455cc1608281820fe2b24f1e5233ade6af1dd5",
         "4fc82b26aecb47d2868c4efbe3581732a3e7cbcc6c2efb32062c08170a05eeb8",
         "6b51d431df5d7f141cbececcf79edf3dd861c3b4069f0b11661a3eefacbba918",
         "3fdba35f04dc8c462986c992bcf875546257113072a909c162f7e470e581e278",
         "8527a891e224136950ff32ca212b45bc93f69fbb801c3b1ebedac52775f99e61"]]  # Depth 3

    test_data_odd = ["7", "8", "9", "10", "11", "12", "13", "14", "15", "16", "17"]
    test_data_odd_hashes = [
        ["15186a28104d094d027f3bd75968b79d84d0a007c02abadb04b391360819f95d"],  # Depth 0
        ["bf57020a599b6ca72c29faca759d2f5c782b0fd1b611ed529e0ea422c28daf36",
         "1f603fb0c570ee1e7eb55b2b2c0feb644aa027a2d3f724471e4ce4e5bd88c770"],  # Depth 1
        ["5e86592667a4d216699c135c5c814b90ebd6083ba5916886e069b8bebfd873ea",
         "5e296e27d6ebc7a30a9ca43974a3a165c11672aeadb0609e3b98238c5a9cc3a2",
         "e4123399f676199e9133231fb35566484f7140f040de1fc7673d30eed6b6f4a6",
         "e4123399f676199e9133231fb35566484f7140f040de1fc7673d30eed6b6f4a6"],  # Depth 2
        ["ada17dcc2d615cc0d982a538b618f45441e798c38b1df9f3dd273e386eba1363",
         "76d4c0b6f7f7cc7122ea4e442c4d2a4af4578855a1dfd3803db52a38b48be8f9",
         "6bce5a3c8b73421b8575f01a0d2c0edb8e2c60eaca11c0452e10597d19bf32a2",
         "65490299ee72d212e57f8b1e48ad29236608927f12c194f5c081717a3342f746",
         "c02b14a7b70952fed80df5f0a99d0a4eaf9705d50b4afeed0a6f4320e4518cba",
         "4d322394fcff4b9b57b536c81c486dda7d16aa2f74ddef9d959c16676d949f82"],  # Depth 3
        ["7902699be42c8a8e46fbbb4501726517e86b22c56a189f7625a6da49081b2451",
         "2c624232cdd221771294dfbb310aca000a0df6ac8b66b696d90ef06fdefb64a3",
         "19581e27de7ced00ff1ce50b2047e7a567c76b1cbaebabe5ef03f7c3017bb5b7",
         "4a44dc15364204a80fe80e9039455cc1608281820fe2b24f1e5233ade6af1dd5",
         "4fc82b26aecb47d2868c4efbe3581732a3e7cbcc6c2efb32062c08170a05eeb8",
         "6b51d431df5d7f141cbececcf79edf3dd861c3b4069f0b11661a3eefacbba918",
         "3fdba35f04dc8c462986c992bcf875546257113072a909c162f7e470e581e278",
         "8527a891e224136950ff32ca212b45bc93f69fbb801c3b1ebedac52775f99e61",
         "e629fa6598d732768f7c726b4b621285f9c3b85303900aa912017db7617d8bdb",
         "b17ef6d19c7a5b1ee83b907c595526dcb1eb06db8227d650d5dda0a9f4ce8cd9",
         "4523540f1504cd17100c4835e85b7eefd49911580f8efff0599a8f283be6b9e3",
         "4523540f1504cd17100c4835e85b7eefd49911580f8efff0599a8f283be6b9e3"]]  # Depth 4

    @patch('source.src.aws_client.DDBClient.save_data')
    @patch('source.src.aws_client.S3Client.save_tree')
    def test_create_new(self, s3_save_tree_mock, ddb_save_data_mock):
        ddb_save_data_mock.return_value = True
        s3_save_tree_mock.return_value = True

        even_tree = MerkleTree.create_new(self.test_data_even)
        self.assertIsNotNone(even_tree, "Tree must be created.")
        self.assertEqual(even_tree.id,
                         self.test_data_even_hashes[0][0],
                         "Tree id did not match.")
        self.assertEqual(len(even_tree.levels), 4, "Tree depth did not match.")
        self.assertEqual(even_tree.size, 15, "Tree size did not match.")

        odd_tree = MerkleTree.create_new(self.test_data_odd)
        self.assertIsNotNone(odd_tree, "Tree must be created.")
        self.assertEqual(odd_tree.id,
                         self.test_data_odd_hashes[0][0],
                         "Tree id did not match.")
        self.assertEqual(len(odd_tree.levels), 5, "Tree depth does not match.")
        self.assertEqual(odd_tree.size, 25, "Tree size does not match.")

    @patch('source.src.aws_client.DDBClient.load_data')
    @patch('source.src.aws_client.S3Client.load_tree')
    def test_load_tree_even(self, s3_load_tree_mock, ddb_load_data_mock):
        s3_load_tree_mock.return_value = self.test_data_even_hashes

        test_leaves_hashes = self.test_data_even_hashes[-1]
        ddb_load_data_mock.return_value = MerkleTreeTest.prepare_mock_data(self.test_data_even,
                                                                           test_leaves_hashes)

        even_tree = MerkleTree.load_tree(self.test_data_even_hashes[0][0])
        self.assertIsNotNone(even_tree, "Expected to successfully load tree.")
        self.assertEqual(even_tree.id, self.test_data_even_hashes[0][0], "Tree id does not match.")
        self.assertEqual(len(even_tree.levels), 4, "Tree depth does not match.")
        self.assertEqual(even_tree.size, 15, "Tree sie does not match.")
        even_hashes = [list(map(lambda node: node.hash, level.nodes)) for level in even_tree.levels]
        self.assertEqual(even_hashes, self.test_data_even_hashes, "Hashes does not match.")

    @patch('source.src.aws_client.DDBClient.load_data')
    @patch('source.src.aws_client.S3Client.load_tree')
    def test_load_tree_odd(self, s3_load_tree_mock, ddb_load_data_mock):
        s3_load_tree_mock.return_value = self.test_data_odd_hashes

        test_leaves_hashes = self.test_data_odd_hashes[-1]
        ddb_load_data_mock.return_value = MerkleTreeTest.prepare_mock_data(self.test_data_odd,
                                                                           test_leaves_hashes)

        even_tree = MerkleTree.load_tree(self.test_data_odd_hashes[0][0])
        self.assertIsNotNone(even_tree, "Expected to successfully load tree.")
        self.assertEqual(even_tree.id, self.test_data_odd_hashes[0][0], "Tree id does not match.")
        self.assertEqual(len(even_tree.levels), 5, "Tree depth does not match.")
        self.assertEqual(even_tree.size, 25, "Tree sie does not match.")
        even_hashes = [list(map(lambda node: node.hash, level.nodes)) for level in even_tree.levels]
        self.assertEqual(even_hashes, self.test_data_odd_hashes, "Hashes does not match.")

    @patch('source.src.aws_client.DDBClient.save_data')
    @patch('source.src.aws_client.S3Client.save_tree')
    def test_index(self, s3_save_tree_mock, ddb_save_data_mock):
        even_tree = MerkleTree.create_new(self.test_data_even)
        for i in range(0, even_tree.size):
            depth, prev_level_nodes = MerkleTreeTest.count_depth_and_max_parent_nodes(i)
            offset = 0 if depth == 0 else (i - prev_level_nodes)
            response = even_tree.index(i)
            self.assertEqual(depth, response['depth'], "Depth is not correct.")
            self.assertEqual(offset, response['offset'], "Offset is not correct.")

    @staticmethod
    def count_depth_and_max_parent_nodes(index):
        # if index <= 0:
        #     return 0, 0
        """
        For the given node index, finds its depth and max nodes possible in parent levels.
        This assumes tree is full and balanced.
        """
        depth = 0
        parent_nodes = 0
        while index > 0:
            max_nodes = pow(2, depth)
            index -= max_nodes
            if index < 0:
                break
            depth += 1
            parent_nodes += max_nodes
        return depth, parent_nodes

    @staticmethod
    def prepare_mock_data(data_list, hash_list):
        # Expect all items in hash_list correspond to items in data_list
        mock_data_map = {}
        for i, hash_val in enumerate(hash_list):
            # Hash list has larger size then use last item of data list for them
            datum = data_list[i] if i < len(data_list) else data_list[-1]
            mock_data_map[hash_val] = datum
        return mock_data_map
