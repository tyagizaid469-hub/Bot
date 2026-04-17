import asyncio
import sys

async def start_process(name, cmd):
    while True:
        print(f"🚀 Starting {name}...")

        try:
            process = await asyncio.create_subprocess_exec(
                sys.executable, cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )

            async def log_stream(stream, prefix):
                while True:
                    line = await stream.readline()
                    if not line:
                        break
                    print(f"[{prefix}] {line.decode().strip()}")

            await asyncio.gather(
                log_stream(process.stdout, name),
                log_stream(process.stderr, name),
            )

        except Exception as e:
            print(f"[{name}] ❌ Process failed to start:", repr(e))

        print(f"❌ {name} crashed or exited. Restarting in 3 sec...")
        await asyncio.sleep(3)

async def main():
    tasks = [
        start_process("MAIN BOT", "bot_app.py"),
        start_process("USERBOT", "userbot.py"),
    ]
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    print("🚀 Run script started...")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("🛑 Run script stopped by user")
        
