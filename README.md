# AmazonLex_ChatBot

The primary purpose of this project is to build a chatbot for booking movie tickets. Chatbot gathers all the required information from the user
in a straightforward and quick conversation, which helps the user to book tickets with ease. Amazon Lex is used for creating a bot, python 
programming language for Lambda code hook, and DynamoDb for fetching details like movie shows, show timings, etc. In this chatbot, the flow of conversation is as follows,
- What genre movies user is looking for
- Select movie, theatre, date, and show timings.
- Select no of tickets, row.
- Confirm the user entered details.
The user input data is also validated. For example, if user has not entered a date or entered a date which is past or a date which is more than
30 days from the booking date, then the bot prompts the error message and asks the user to enter a valid date. Similarly, the validation is for all
the responses from the users.
