import openai
import os

openai.api_key = os.environ['openai_key']

def ask_chatgpt(question):

    msg = [
        {
            "role": "user",
            "content": question
        }
    ]
    try:
        # APIにリクエストを送信
        response = openai.chat.completions.create(
            model = "gpt-3.5-turbo",  
            messages = msg,
        )
        return response.choices[0].message.content
    except Exception as e:
        return f"OpenAI APIのエラー: {e}"  # 詳細なエラーメッセージを表示


def modify_script():
    with open('script.csv','r',encoding='utf-8') as file:
        csvcontent = file.read()         
    with open('prompt_for_modifyscript.md','r',encoding='utf-8') as file:
        prompt = file.read()
    prompt = f"""
        {prompt}

        以下がCSV形式の台本です
        ```
        {csvcontent}
        ```
    """
    return ask_chatgpt(prompt)

if __name__ == "__main__":
    response = modify_script()
    print(response)