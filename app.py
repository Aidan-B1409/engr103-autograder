import argparse
import os
import re
from typing import List, Tuple

import pandas as pd
from apiclient import discovery
from canvasapi import Canvas, canvas
from dotenv import load_dotenv
from flatten_json import flatten
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials, credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from httplib2 import Http
from thefuzz import fuzz

load_dotenv()

API_KEY = os.getenv("CANVAS_TOKEN")
API_URL = "https://canvas.oregonstate.edu"
COURSE_ID = 1958944
SCOPES = [
    "https://www.googleapis.com/auth/forms.body.readonly",
    "https://www.googleapis.com/auth/forms.responses.readonly",
]
DISCOVERY_DOC = "https://forms.googleapis.com/$discovery/rest?version=v1"
FORM_ID = "1Bv5DmbjrqN3rqHXXs22Nu85tOUcAaFpGR8ap7MnkGZg"
FUZZ_THRESHOLD = 70


# Google API authentication
class AttendanceForm:
    def __init__(self, form_id=FORM_ID) -> None:
        self.api_service = self.get_forum_API()
        self.form_id = form_id

    def get_forum_API(self):
        creds = None
        # The file token.json stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        if os.path.exists("token.json"):
            creds = Credentials.from_authorized_user_file("token.json", SCOPES)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    "client_secrets.json", SCOPES
                )
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open("token.json", "w") as token:
                token.write(creds.to_json())

        return discovery.build("forms", "v1", credentials=creds)

    def get_form_body(self) -> dict:
        return self.api_service.forms().get(formId=self.form_id).execute()

    def get_form_responses(self) -> dict:
        return list(
            map(
                flatten,
                self.api_service.forms()
                .responses()
                .list(formId=self.form_id)
                .execute()["responses"],
            )
        )

    def get_question_ids(self, body: dict) -> dict:
        return {
            x["questionItem"]["question"]["questionId"]: x["title"]
            for x in body["items"]
        }

    def clean_column_index(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
        # Drop Question ID columns
        df = df.drop(
            columns=[x for x in df.columns.tolist() if x.endswith("questionId")]
        )
        # Create a list of columns which assosciate to a question ID
        col_keys = [x for x in df.columns.tolist() if "_" in x]
        # Extract only the question ID from the column name
        col_keys = list(
            map(lambda x: re.findall(r"_[^_]*_", x)[0].replace("_", ""), col_keys)
        )
        # Concatenate with un-ID'd columns and reset the column index in the dataframe
        df.columns = df.columns.tolist()[:4] + col_keys
        return df, col_keys

    def get_attendance(self) -> pd.DataFrame:
        df = pd.DataFrame.from_dict(self.get_form_responses())
        df, keys = self.clean_column_index(df)
        # Get titles of current columns
        q_titles = self.get_question_ids(self.get_form_body())
        # Drop deprecated columns that don't match current form
        dropcols = [k for k in keys if k not in q_titles.keys()]
        df = df.drop(columns=dropcols)
        df.columns = [q_titles[x] if x in q_titles.keys() else x for x in df.columns]
        return df


class CanvasCourse:
    def __init__(self) -> None:
        self.course = self.get_course()

    # Canvas API authentication
    def get_course(self) -> canvas.Course:
        canvas = Canvas(API_URL, API_KEY)
        course = canvas.get_course(COURSE_ID)
        print(f"Connecting to {course.name}")
        return course

    def canvas_upload(self, df: pd.DataFrame, ass_id: int):
        print("Generating user list! This might take a while...")
        users = self.course.get_users(enrollment_type=["student"])
        uids = pd.DataFrame.from_records(list(map(lambda x: x.get_profile(), users)))

        assignment = self.course.get_assignment(ass_id)

        # Match all users from uids who were identified in attendance
        absent = uids[~uids["login_id"].isin(df["respondentEmail"])]
        absent.apply(lambda x: self.set_score(x, assignment, 0), axis=1)

        present = uids[uids["login_id"].isin(df["respondentEmail"])]
        present.apply(lambda x: self.set_score(x, assignment, 1), axis=1)

    def set_score(self, student: pd.Series, assignment, score: int) -> None:
        userinfo = "Marking absent: " if score == 0 else "Marking present: "
        print(f"{userinfo}{student['name']}")
        submission = assignment.get_submission(student["id"])
        submission.edit(submission={"posted_grade": score})


# Retrieve all attendance form submissions on a given day that are submitted within the lecture period
# WARN: Lecture period is currently hard-coded to be between 1:00pm and 2:00pm PST
# Future maintainers please check your lecture schedules and update these timestamps.
# TODO: Move these timestamps into a config file
# INFO: Google Forms returns our timestamps in Zulu time, so please convert from PST to Zulu in your timestamp range.
def filter_by_day(df: pd.DataFrame, date: str) -> pd.DataFrame:
    df = df.set_index(pd.DatetimeIndex(df["lastSubmittedTime"]))
    return df.sort_index().loc[f"{date}T19:00:00":f"{date}T20:00:00", :]


def filter_by_passphrase(df: pd.DataFrame, passphrase: str) -> pd.DataFrame:
    ckey = "What is the Concept of the Day?"
    df[ckey] = list(map(lambda x: x.strip().lower(), df[ckey]))

    return df[
        df.apply(
            lambda x: fuzz.partial_token_set_ratio(x[ckey], passphrase)
            >= FUZZ_THRESHOLD,
            axis=1,
        )
    ]


def identify_assistance_need(df: pd.DataFrame):
    matches = ["Help", "Understanding", "Speed"]
    num_cols = df.columns[
        df.columns.to_series().apply(lambda s: any(x in s for x in matches))
    ]
    return df[df[num_cols.tolist()].lt(2).any(axis=1)]


def convert_numeric_cols(df: pd.DataFrame):
    matches = ["hours", "Help", "Understanding", "Speed"]
    num_cols = df.columns[
        df.columns.to_series().apply(lambda s: any(x in s for x in matches))
    ]
    df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce", axis=1)
    return df


def parsing_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="A system for automatically grading student attendance grades from a Google Form."
    )
    parser.add_argument(
        "--ass_id",
        type=int,
        help="Assignment ID of the lecture attendance assignment on Canvas.",
    )
    parser.add_argument("--date", help="Date of the lecture in YYYY-MM-DD format.")
    parser.add_argument(
        "--keyphrase",
        help="Keyphrase presented in class. We will fuzzy search on this keyphrase.",
    )
    return parser.parse_args()


# TODO: Check for existing grades
def main():
    args = parsing_args()
    keyphrase = args.keyphrase.strip().lower()
    canvas = CanvasCourse()
    attendance_form = AttendanceForm()

    # Pull attendance responses from Google Forms
    attend_df = attendance_form.get_attendance()
    # Select only those matching our timestamp
    attend_df = filter_by_day(attend_df, args.date)
    # Select only those roughly matching our passphrase
    attend_df = filter_by_passphrase(attend_df, keyphrase)

    # Convert columns to numeric
    attend_df = convert_numeric_cols(attend_df)
    # Compute averages across all numerical columns, save to report file
    report = attend_df.select_dtypes("number").mean()
    report.to_csv(f"{args.date}_report.csv")

    # Compute students who need help
    support_df = identify_assistance_need(attend_df)
    print("Students in need of support: \n")
    print(support_df["respondentEmail"])

    # Submit scores to Canvas
    canvas.canvas_upload(attend_df, args.ass_id)

    attend_df.to_csv(f"{args.date}_out.csv")


if __name__ == "__main__":
    main()
