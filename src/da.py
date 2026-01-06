import matplotlib.pyplot as plt

# număr de thread-uri
p = [1, 2, 3, 4, 5, 6, 7, 8]

# eficiențele medii E(p)
E = [13.561, 8.962, 7.475, 6.633, 6.450, 6.6440,5.968, 5.514]

plt.figure()
plt.plot(p, E, marker="o")
plt.xlabel("Număr de thread-uri (p)")
plt.ylabel("Timp de excuție")
plt.title("Timpul de execuție în funcție de numărul de thread-uri")
plt.grid(True)
plt.xticks(p)

plt.tight_layout()
plt.savefig("efficiency.png", dpi=300)
# plt.show()  # dacă vrei să-l vezi direct
