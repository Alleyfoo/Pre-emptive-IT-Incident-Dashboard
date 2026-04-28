# The Computer Helper Robot

Hi! This is a friendly robot that watches lots of computers to make sure they are happy and healthy.

## What does it do?

Think about a teacher who watches kids on the playground. If a kid trips and falls, the teacher runs over to help. Our robot is like that, but for computers!

1. The robot **looks** at computers and sees how they are feeling.
2. If a computer is sick or sad, the robot says "Uh oh!"
3. The robot writes everything down in a notebook so we can read it later.
4. We can look at a big picture board (called a dashboard) to see all the computers at once.

## How do I play with it on my own computer?

You need a few things first, like a kid needs crayons before they color:

- A program called **Python** (it makes the robot work).
- A program called **git** (it brings the robot's pieces to your computer).

Then you type these magic words:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r demos/requirements-demo.txt
```

Now make some pretend computer problems and let the robot find them:

```powershell
python -m tools.generate_ticket_scenarios --run-id demo
python -m runtime.incident_flow --run-id demo
python -m tools.validate --run-id demo
```

And to see the pretty picture board:

```powershell
streamlit run demos/streamlit_incident_dashboard.py
```

The robot puts all its notes in a folder called `artifacts/`. That is the robot's notebook!

## Using a big box called Docker

Docker is like a lunchbox that already has everything inside. You just open it and eat!

```bash
docker compose up --build
```

That one line opens the lunchbox and the robot starts working. Yum!

## Putting the robot in the sky (the Cloud)

The cloud is just other people's computers far far away. We can send our robot there too, so it works even when your computer is sleeping.

If you want to do this, two grown-up books will help you:

- `docs/DEPLOY_CLOUD_RUN.md` — the quick book.
- `docs/DEPLOY_PRODUCTION.md` — the big book with all the details.

## Where does the robot get its information?

Three ways, like three different snacks:

- **Pretend snacks** — we make fake computer problems for practice.
- **Real snacks** — real computers send their report cards to the robot.
- **A little helper** — a tiny program on Windows computers that fills out the report card for them.

## Safety rules

The robot is careful and follows these rules:

- It checks every paper to make sure it is filled out the right way.
- It hides secret things so no one peeks.
- Only one robot works at a time, so they do not bonk into each other.
- It throws away old notes it does not need anymore (but keeps the important ones).

The end!
