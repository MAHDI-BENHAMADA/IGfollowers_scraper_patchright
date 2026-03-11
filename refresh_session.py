import instaloader

L = instaloader.Instaloader()
L.interactive_login("kimodrac")
L.save_session_to_file("ig_session")
print("Session saved successfully.")
