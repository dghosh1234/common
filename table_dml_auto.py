Option Explicit

Dim tbl, col, semantics, cleanLength
Dim output, discrepFound, discrepCount
Dim fso, outFile, filePath

output = "Discrepancy Report: VARCHAR2 columns with BYTE semantics" & vbCrLf & vbCrLf
discrepFound = False
discrepCount = 0

For Each tbl In ActiveModel.Tables
    If Not tbl.IsShortcut Then
        For Each col In tbl.Columns
            If InStr(UCase(col.DataType), "VARCHAR2") > 0 Then
                On Error Resume Next
                semantics = col.GetExtendedAttribute("LengthSemantics")
                If Err.Number <> 0 Then
                    semantics = "(not available)"
                    Err.Clear
                End If
                On Error GoTo 0

                cleanLength = col.Length

                If UCase(semantics) <> "CHAR" Then
                    discrepFound = True
                    discrepCount = discrepCount + 1
                    output = output & "Table: " & tbl.Code & vbCrLf
                    output = output & "  Column: " & col.Code & vbCrLf
                    output = output & "    DataType: " & col.DataType & vbCrLf
                    output = output & "    Length: " & cleanLength & vbCrLf
                    output = output & "    Semantics: " & semantics & vbCrLf
                    output = output & "    ❗ Suggestion: Change to VARCHAR2(" & cleanLength & " CHAR)" & vbCrLf & vbCrLf
                End If
            End If
        Next
    End If
Next

If discrepFound Then
    Set fso = CreateObject("Scripting.FileSystemObject")
    filePath = "C:\Users\Public\varchar2_semantics_report.txt" ' Change this path if needed
    Set outFile = fso.CreateTextFile(filePath, True)
    outFile.Write output
    outFile.Close
    MsgBox "Discrepancy report generated (" & discrepCount & " issues found)." & vbCrLf & "Saved to: " & filePath, vbInformation, "Report Complete"
Else
    MsgBox "No VARCHAR2 columns with BYTE semantics found in the model.", vbInformation, "All Good"
End If
