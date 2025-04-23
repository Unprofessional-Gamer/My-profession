def longest_common_subsequence(s1, s2):
    m, n = len(s1), len(s2)
    dp = [["" for _ in range(n + 1)] for _ in range(m + 1)]

    for i in range(m):
        for j in range(n):
            if s1[i] == s2[j]:
                dp[i + 1][j + 1] = dp[i][j] + s1[i]
            else:
                dp[i + 1][j + 1] = max(dp[i + 1][j], dp[i][j + 1], key=len)

    return dp[m][n]

# Read input line, process and print result
line = input().strip()
if line:
    str1, str2 = line.split(":")
    result = longest_common_subsequence(str1, str2)
    print(result)