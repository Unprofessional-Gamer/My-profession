import random

# Define the board
board = [i for i in range(101)]

# Snakes and ladders as a dictionary where key is the start and value is the end
snakes = {16: 6, 47: 26, 49: 11, 56: 53, 62: 19, 64: 60, 87: 24, 93: 73, 95: 75, 98: 78}
ladders = {1: 38, 4: 14, 9: 31, 21: 42, 28: 84, 36: 44, 51: 67, 71: 91, 80: 100}

# Function to roll the dice
def roll_dice():
    return random.randint(1, 6)

# Function to move player
def move_player(player_position):
    dice_value = roll_dice()
    player_position += dice_value
    if player_position > 100:
        player_position -= dice_value  # If move goes beyond 100, stay in the same place
    elif player_position in snakes:
        print(f"Oops! Bitten by a snake at {player_position}")
        player_position = snakes[player_position]
    elif player_position in ladders:
        print(f"Yay! Climbed a ladder at {player_position}")
        player_position = ladders[player_position]
    return player_position

# Main game loop
def play_game():
    player1_position = 0
    player2_position = 0
    turn = 0

    while player1_position < 100 and player2_position < 100:
        if turn % 2 == 0:
            print("Player 1's turn")
            player1_position = move_player(player1_position)
            print(f"Player 1 is now at {player1_position}")
        else:
            print("Player 2's turn")
            player2_position = move_player(player2_position)
            print(f"Player 2 is now at {player2_position}")
        turn += 1

    if player1_position >= 100:
        print("Player 1 wins!")
    else:
        print("Player 2 wins!")

# Start the game
play_game()
