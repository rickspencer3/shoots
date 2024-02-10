import concurrent.futures
import unittest
import sys

def run_test_case(test_case):
    suite = unittest.TestLoader().loadTestsFromTestCase(test_case)
    runner = unittest.TextTestRunner()
    runner.run(suite)

if __name__ == "__main__":
    from tls_test import TLSTest
    from insecure_test import InsecureTest
    from jwt_test import JWTTest

    test_cases = [TLSTest, JWTTest, InsecureTest]

    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(run_test_case, test_cases)
