# q_reports

How to run the script?

The script can run in 2 modes, current and list

- Current is the mode that will get report as csv with the current datetime (maximum timestamp from filename, that is less than the current time)

    python main.py --mode current

- List is the mode that will get report as csv with the specific list of datetime (maximum timestamp from filename, that is less than the current time)
    python main.py --mode list --timestamps "2024-01-15 12:00:00" "2024-04-15 12:00:00"

    Note that
        - timestamp is a string in Year-Month-Date Hour:Minute:Second format
        - List of timestamps can be entered by put string of datetime one after another one as example above