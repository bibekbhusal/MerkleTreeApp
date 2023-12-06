import hashlib
import json
from typing import Dict, List

from aws_client import DDBClient, S3Client


class HashLib:
    """
    A utility class to hash objects.
    """

    @staticmethod
    def hash_str(content: str):
        """
        Returns SHA-256 has of the proved content.
        :param content: str to hash
        :return: sha-256 hash
        """
        return hashlib.sha256(content.encode("utf-8")).hexdigest()


class MerkleNode:
    """
    Represent non-leaf nodes of the Merkle Tree.
    """

    def __init__(self, left, right, hash_val):
        self.left = left
        self.right = right
        self.hash = hash_val

    def __str__(self):
        return json.dumps(self.__dict__)


class MerkleLeafNode(MerkleNode):
    """
    Represents leaf nodes of the Merkle Tree.
    """

    # TODO: Future support: Have content support dynamic type instead of just string.
    def __init__(self, hash_val, content: str):
        super().__init__(None, None, hash_val)
        self.content = content


class MerkleLevel:
    """
    Represent a level (nodes in a depth) of the Merkle Tree.
    """

    def __init__(self, identifier: int, nodes: List[MerkleNode]):
        self.id = identifier  # Depth of the level.
        self.nodes = nodes

    def __str__(self):
        return json.dumps(self.__dict__)

    def __len__(self):
        return self.size()

    def size(self) -> int:
        """
        :return: Size of the level, aka number of nodes in the level.
        """
        return len(self.nodes)

    def offset(self, offset: int) -> MerkleNode:
        """
        Returns a node at the given offset in the nodes of the level.
        :param offset: 0-indexed position (from left-to-right) of the node list of the level.
        :return: Node at the given offset, if it is within bounds.
        """
        if offset >= len(self.nodes):
            raise ValueError(f"Offset cannot be larger than size of nodes. "
                             f"Given: {offset}, number of nodes: {len(self.nodes)}")
        return self.nodes[offset]


class MerkleTree:

    def __init__(self, identifier: str, levels: List[MerkleLevel]):
        self.id = identifier
        self.levels = levels
        self.size = sum(map(lambda level: len(level), levels))

    def __str__(self):
        return json.dumps(self.__dict__)

    def index(self, index: int) -> Dict[str, str]:
        """
        Returns a dictionary of information of the node at the given index in the tree.
        :param index: 0-indexed position of a node in the tree, from top-to-bottom and left-to-right
        :return: A dictionary of information of the node at the given index, if it is within bound.
        """
        if index < 0 or index >= self.size:
            raise ValueError(f"Index must be within bounds of the tree nodes size. "
                             f"Given: {index} is outside the valid range: [0, {self.size - 1}].")
        depth = 0
        offset = index
        node = None
        for level in self.levels:
            level_size = level.size()
            if offset < level_size:
                node = level.nodes[offset]
                break
            depth += 1
            offset -= level_size

        value = node.hash
        if isinstance(node, MerkleLeafNode):
            value = node.content

        return {"depth": depth, "offset": offset, "value": value}

    @classmethod
    def create_new(cls, data: List[str]):
        """
        Creates a new tree using the data list provided. Data is persisted in DynamoDB and tree is persisted in S3.
        :param data: List of data (leaves)
        :return: A new instance of Merkle Tree.
        """

        if len(data) <= 0:
            raise ValueError("Data list must be non-empty.")

        data_map = {}
        for datum in data:
            hash_val = HashLib.hash_str(datum)
            data_map[hash_val] = datum

        # Save data in DynamoDB
        DDBClient.save_data(data_map)

        children = [MerkleLeafNode(hash_val, datum) for hash_val, datum in data_map.items()]

        if len(children) % 2 != 0:
            children.append(children[-1])

        level_nodes = [children]

        while len(children) > 1:
            next_level = []
            if len(children) % 2 != 0:
                children.append(children[-1])

            for i in range(0, len(children), 2):
                left = children[i]
                right = children[i + 1]
                hash_val = HashLib.hash_str(left.hash + right.hash)
                next_level.append(MerkleNode(left, right, hash_val))

            level_nodes.append(next_level)
            children = next_level

        root_id = children[0].hash

        # Since levels were created bottom-up, reverse iterate to put root node at the top
        levels = [MerkleLevel(i, nodes) for i, nodes in reversed(list(enumerate(level_nodes)))]

        hashes_list = [list(map(lambda node: node.hash, level.nodes)) for level in levels]
        # Save tree in S3
        S3Client.save_tree(root_id, hashes_list)

        return cls(root_id, levels)

    @classmethod
    def load_tree(cls, tree_id: str):
        """
        Loads a tree with the given id from persistence store (S3 persists tree, DynamoDB persist data)
        :param tree_id: Tree identifier
        :return: Tree from data loaded from persistence store.
        """

        print(f"Loading tree with id: {tree_id}")

        # Load tree state from S3
        hashes_list = S3Client.load_tree(tree_id)
        print(f"Received hash list from S3: {hashes_list}")

        if len(hashes_list) < 2:
            raise ValueError(f"Expected at least 3 nodes in the tree. Found: {len(hashes_list)}")

        # Selecting only unique values
        leaves = list(set(hashes_list[-1]))

        # Load tree leaves data from DynamoDB
        data_map = DDBClient.load_data(leaves)
        if len(data_map) == 0:
            raise ValueError(f"Data map from DynamoDB is empty.")

        return cls.build_tree(hashes_list, data_map)

    @classmethod
    def build_tree(cls, hashes_list: List[List[str]], data_map: Dict[str, str]):
        """
        Builds tree from given list of list hash and data map.

        :param hashes_list: List of list hashes of nodes in each level of the tree.
        :param data_map: Dictionary of hash -> data of leaf nodes.
        :return: Created Merkle tree from provided data
        """
        if len(hashes_list) < 2:
            raise ValueError(f"Expected at least 3 nodes in the tree. Found: {len(hashes_list)}")

        if len(data_map) == 0:
            raise ValueError(f"Data map must be non-empty.")

        children = [MerkleLeafNode(hash_val, data_map[hash_val]) for hash_val in hashes_list[-1]]

        level_nodes: List[List[MerkleNode]] = [children]

        for level, hashes in enumerate(reversed(hashes_list[:-1])):
            if len(children) == 0 or len(children) % 2 != 0:
                raise ValueError(f"Expected even number of children for the level: {level} but found: {len(children)}")

            parents = {}
            for i in range(0, len(children), 2):
                left = children[i]
                right = children[i + 1]
                hash_val = hashes[i // 2]
                parents[hash_val] = MerkleNode(left, right, hash_val)

            parent_nodes = [parents[hash_val] for hash_val in hashes]

            if len(hashes) != len(parent_nodes):
                raise ValueError(f"Excepted to same number of parent nodes as number of hashes. "
                                 f"Created: {len(parent_nodes)}, needed: {len(hashes)}")

            level_nodes.append(parent_nodes)
            children = parent_nodes

        root_id = children[0].hash

        # Since levels were created bottom-up, reverse iterate to put root node at the top
        levels = [MerkleLevel(i, nodes) for i, nodes in reversed(list(enumerate(level_nodes)))]
        return cls(root_id, levels)
